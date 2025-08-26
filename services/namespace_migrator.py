import os
import logging
import time
import json
from typing import Dict, Any, List, Optional, Tuple
from pinecone import Pinecone
from datetime import datetime

logger = logging.getLogger(__name__)


class NamespaceMigrator:
    """
    Service for migrating Pinecone records from metadata-based record_type 
    to namespace-based organization.
    """
    
    def __init__(self, dry_run: bool = False):
        """
        Initialize the Namespace Migrator.
        
        Args:
            dry_run: If True, only log operations without making changes
        """
        self.api_key = os.environ.get('PINECONE_API_KEY')
        self.environment = os.environ.get('PINECONE_ENVIRONMENT')
        self.index_name = os.environ.get('PINECONE_INDEX_NAME')
        self.dry_run = dry_run
        
        # Migration configuration
        self.batch_size = int(os.environ.get('MIGRATION_BATCH_SIZE', '100'))
        self.rate_limit = int(os.environ.get('MIGRATION_RATE_LIMIT', '10'))
        self.rate_limit_delay = 1.0 / self.rate_limit  # Delay between operations
        
        if not all([self.api_key, self.environment, self.index_name]):
            raise ValueError("Missing required Pinecone environment variables")
        
        try:
            logger.info(f"Initializing Pinecone client for migration (dry_run={dry_run})")
            self.pinecone = Pinecone(api_key=self.api_key, environment=self.environment)
            self.index = self.pinecone.Index(self.index_name)
            logger.info(f"Connected to Pinecone index: {self.index_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Pinecone client: {e}")
            raise
    
    def inventory_record_types(self, namespace: str = "", sample_size: int = 5000) -> Dict[str, Any]:
        """
        Scan existing records to inventory record_types and estimate work.
        
        Args:
            namespace: The namespace to scan (empty string for default)
            sample_size: Number of records to sample for analysis
            
        Returns:
            Dict containing inventory results
        """
        try:
            logger.info(f"Starting inventory of record types in namespace '{namespace}'")
            
            # Get index stats to estimate total records
            stats = self.index.describe_index_stats()
            namespace_stats = stats.get('namespaces', {}).get(namespace, {})
            total_vectors = namespace_stats.get('vector_count', 0)
            
            logger.info(f"Total vectors in namespace '{namespace}': {total_vectors}")
            
            # Sample records to analyze record_types
            record_types = {}
            sampled_count = 0
            next_page_token = None
            
            while sampled_count < min(sample_size, total_vectors):
                # Rate limiting
                time.sleep(self.rate_limit_delay)
                
                # List vectors with pagination using list_paginated for explicit control
                list_kwargs = {
                    'namespace': namespace,
                    'limit': min(self.batch_size, sample_size - sampled_count)
                }
                if next_page_token:
                    list_kwargs['pagination_token'] = next_page_token
                    
                list_response = self.index.list_paginated(**list_kwargs)
                
                # Log response structure for debugging
                logger.info(f"List response type: {type(list_response)}")
                
                # Extract vector IDs from the response
                vector_list = list_response.vectors if hasattr(list_response, 'vectors') else []
                if not vector_list:
                    logger.warning("No vectors found in list response")
                    break
                
                # Extract actual ID strings from vector objects
                # The list_paginated returns vector objects with an 'id' attribute
                vector_ids = [v.id if hasattr(v, 'id') else str(v) for v in vector_list]
                
                logger.info(f"Found {len(vector_ids)} vector IDs to fetch")
                if vector_ids:
                    logger.info(f"Sample vector ID: {vector_ids[0]}")
                
                ids_to_fetch = vector_ids
                
                # Fetch vector metadata
                time.sleep(self.rate_limit_delay)
                fetch_response = self.index.fetch(
                    ids=ids_to_fetch,
                    namespace=namespace
                )
                
                # Log fetch response structure
                logger.info(f"Fetch response type: {type(fetch_response)}")
                logger.info(f"Fetch response attributes: {dir(fetch_response)}")
                
                # Analyze record types
                vectors = fetch_response.vectors if hasattr(fetch_response, 'vectors') else {}
                logger.info(f"Fetched {len(vectors)} vectors with metadata")
                
                for vector_id, vector_data in vectors.items():
                    # vector_data is a Vector object, not a dict
                    metadata = vector_data.metadata if hasattr(vector_data, 'metadata') else {}
                    record_type = metadata.get('record_type', 'unknown') if metadata else 'unknown'
                    
                    # Log first few metadata samples for debugging
                    if sampled_count < 3:
                        logger.info(f"Vector {vector_id} metadata keys: {list(metadata.keys())}")
                        if 'record_type' in metadata:
                            logger.info(f"Vector {vector_id} record_type: {metadata['record_type']}")
                    
                    if record_type not in record_types:
                        record_types[record_type] = {
                            'count': 0,
                            'sample_ids': [],
                            'sample_metadata': []
                        }
                    
                    record_types[record_type]['count'] += 1
                    
                    # Keep sample data
                    if len(record_types[record_type]['sample_ids']) < 5:
                        record_types[record_type]['sample_ids'].append(vector_id)
                        record_types[record_type]['sample_metadata'].append(metadata)
                
                sampled_count += len(vector_ids)
                # Get next pagination token from response
                next_page_token = getattr(list_response, 'pagination_token', None)
                
                if not next_page_token:
                    break
                
                logger.info(f"Sampled {sampled_count}/{sample_size} records")
            
            # Extrapolate to total population
            if sampled_count > 0:
                extrapolation_factor = total_vectors / sampled_count
                for record_type in record_types:
                    record_types[record_type]['estimated_total'] = int(
                        record_types[record_type]['count'] * extrapolation_factor
                    )
            
            inventory_result = {
                'total_vectors': total_vectors,
                'sampled_count': sampled_count,
                'record_types': record_types,
                'source_namespace': namespace,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Inventory complete: Found {len(record_types)} record types")
            return inventory_result
            
        except Exception as e:
            logger.error(f"Error during inventory: {e}")
            raise
    
    def migrate_batch(self, 
                     vector_ids: List[str],
                     source_namespace: str,
                     target_namespace_map: Dict[str, str],
                     rollback_data: List[Dict] = None) -> Tuple[int, int, List[Dict]]:
        """
        Migrate a batch of vectors to new namespaces based on their record_type.
        
        Args:
            vector_ids: List of vector IDs to migrate
            source_namespace: Source namespace
            target_namespace_map: Mapping of record_type to target namespace
            rollback_data: List to append rollback information
            
        Returns:
            Tuple of (success_count, failed_count, rollback_data)
        """
        success_count = 0
        failed_count = 0
        rollback_data = rollback_data or []
        
        try:
            # Fetch vectors from source namespace
            time.sleep(self.rate_limit_delay)
            fetch_response = self.index.fetch(
                ids=vector_ids,
                namespace=source_namespace
            )
            
            vectors_by_namespace = {}
            
            # Group vectors by target namespace
            vectors = fetch_response.vectors if hasattr(fetch_response, 'vectors') else {}
            for vector_id, vector_data in vectors.items():
                # vector_data is a Vector object, not a dict
                metadata = vector_data.metadata if hasattr(vector_data, 'metadata') else {}
                record_type = metadata.get('record_type', 'unknown') if metadata else 'unknown'
                
                # Dynamic namespace creation based on record_type
                # If target_namespace_map is None or empty, use dynamic naming
                if not target_namespace_map or target_namespace_map.get('use_dynamic', False):
                    # Pluralize common types, otherwise use as-is
                    pluralize_map = {
                        'donation': 'donations',
                        'bid': 'bids',
                        'purchase': 'purchases',
                        'item': 'items',
                        'event': 'events',
                        'user': 'users'
                    }
                    # Use pluralized form if in map, otherwise add 's' if not already plural
                    if record_type in pluralize_map:
                        target_namespace = pluralize_map[record_type]
                    elif record_type.endswith('s'):
                        target_namespace = record_type
                    else:
                        target_namespace = f"{record_type}s"
                else:
                    # Use provided mapping with fallback
                    target_namespace = target_namespace_map.get(record_type, f"migrated_{record_type}")
                
                if target_namespace not in vectors_by_namespace:
                    vectors_by_namespace[target_namespace] = []
                
                # Prepare vector for upsert (remove record_type from metadata)
                new_metadata = {k: v for k, v in metadata.items() if k != 'record_type'}
                new_metadata['migration_timestamp'] = datetime.utcnow().isoformat()
                new_metadata['source_namespace'] = source_namespace
                
                # Access values attribute directly from Vector object
                vector_values = vector_data.values if hasattr(vector_data, 'values') else None
                
                vectors_by_namespace[target_namespace].append({
                    'id': vector_id,
                    'values': vector_values,
                    'metadata': new_metadata
                })
                
                # Track for rollback
                rollback_data.append({
                    'vector_id': vector_id,
                    'source_namespace': source_namespace,
                    'target_namespace': target_namespace,
                    'original_metadata': metadata
                })
            
            # Upsert to target namespaces
            for target_namespace, vectors in vectors_by_namespace.items():
                if self.dry_run:
                    logger.info(f"[DRY RUN] Would upsert {len(vectors)} vectors to namespace '{target_namespace}'")
                    success_count += len(vectors)
                else:
                    try:
                        time.sleep(self.rate_limit_delay)
                        self.index.upsert(
                            vectors=vectors,
                            namespace=target_namespace
                        )
                        success_count += len(vectors)
                        logger.info(f"Successfully migrated {len(vectors)} vectors to namespace '{target_namespace}'")
                    except Exception as e:
                        logger.error(f"Failed to upsert to namespace '{target_namespace}': {e}")
                        failed_count += len(vectors)
            
            return success_count, failed_count, rollback_data
            
        except Exception as e:
            logger.error(f"Error migrating batch: {e}")
            failed_count += len(vector_ids)
            return success_count, failed_count, rollback_data
    
    def verify_migration(self,
                        source_namespace: str,
                        target_namespaces: List[str],
                        sample_size: int = 100) -> Dict[str, Any]:
        """
        Verify successful migration by comparing record counts and sample vectors.
        
        Args:
            source_namespace: Original namespace
            target_namespaces: List of target namespaces
            sample_size: Number of vectors to sample for verification
            
        Returns:
            Dict containing verification results
        """
        try:
            logger.info("Starting migration verification")
            
            # Get stats for all namespaces
            stats = self.index.describe_index_stats()
            
            source_count = stats.get('namespaces', {}).get(
                source_namespace or 'default', {}
            ).get('vector_count', 0)
            
            target_counts = {}
            total_target_count = 0
            
            for namespace in target_namespaces:
                count = stats.get('namespaces', {}).get(namespace, {}).get('vector_count', 0)
                target_counts[namespace] = count
                total_target_count += count
            
            # Sample verification
            sample_results = []
            for namespace in target_namespaces[:3]:  # Sample first 3 namespaces
                time.sleep(self.rate_limit_delay)
                list_response = self.index.list_paginated(
                    namespace=namespace,
                    limit=min(10, sample_size)
                )
                
                # Extract vector list and convert to IDs
                vector_list = list_response.vectors if hasattr(list_response, 'vectors') else []
                vector_ids = [v.id if hasattr(v, 'id') else str(v) for v in vector_list]
                if vector_ids:
                    time.sleep(self.rate_limit_delay)
                    fetch_response = self.index.fetch(
                        ids=vector_ids[:5],
                        namespace=namespace
                    )
                    
                    vectors = fetch_response.vectors if hasattr(fetch_response, 'vectors') else {}
                    for vector_id, vector_data in vectors.items():
                        # vector_data is a Vector object, not a dict
                        metadata = vector_data.metadata if hasattr(vector_data, 'metadata') else {}
                        sample_results.append({
                            'namespace': namespace,
                            'vector_id': vector_id,
                            'has_migration_timestamp': 'migration_timestamp' in metadata,
                            'has_source_namespace': 'source_namespace' in metadata,
                            'no_record_type': 'record_type' not in metadata
                        })
            
            verification_result = {
                'source_namespace': source_namespace,
                'source_count': source_count,
                'target_namespaces': target_counts,
                'total_target_count': total_target_count,
                'count_match': source_count == total_target_count,
                'sample_verification': sample_results,
                'verification_timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Verification complete: Source={source_count}, Target={total_target_count}")
            return verification_result
            
        except Exception as e:
            logger.error(f"Error during verification: {e}")
            raise
    
    def generate_resume_token(self, last_processed_id: str, namespace: str) -> str:
        """
        Generate a resume token for interrupted migrations.
        
        Args:
            last_processed_id: Last successfully processed vector ID
            namespace: Current namespace being processed
            
        Returns:
            Base64 encoded resume token
        """
        import base64
        token_data = {
            'last_id': last_processed_id,
            'namespace': namespace,
            'timestamp': datetime.utcnow().isoformat()
        }
        token_json = json.dumps(token_data)
        return base64.b64encode(token_json.encode()).decode()
    
    def parse_resume_token(self, token: str) -> Dict[str, Any]:
        """
        Parse a resume token to continue migration.
        
        Args:
            token: Base64 encoded resume token
            
        Returns:
            Dict containing resume information
        """
        import base64
        try:
            token_json = base64.b64decode(token.encode()).decode()
            return json.loads(token_json)
        except Exception as e:
            logger.error(f"Failed to parse resume token: {e}")
            return {}
    
    def rollback_migration(self, rollback_data: List[Dict]) -> Dict[str, Any]:
        """
        Rollback a migration using tracked changes.
        
        Args:
            rollback_data: List of rollback information
            
        Returns:
            Dict containing rollback results
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Would rollback {len(rollback_data)} vectors")
            return {
                'rollback_count': len(rollback_data),
                'dry_run': True
            }
        
        success_count = 0
        failed_count = 0
        
        try:
            # Group by target namespace for batch deletion
            vectors_by_namespace = {}
            for item in rollback_data:
                target_ns = item['target_namespace']
                if target_ns not in vectors_by_namespace:
                    vectors_by_namespace[target_ns] = []
                vectors_by_namespace[target_ns].append(item['vector_id'])
            
            # Delete from target namespaces
            for namespace, vector_ids in vectors_by_namespace.items():
                try:
                    time.sleep(self.rate_limit_delay)
                    self.index.delete(
                        ids=vector_ids,
                        namespace=namespace
                    )
                    success_count += len(vector_ids)
                    logger.info(f"Rolled back {len(vector_ids)} vectors from namespace '{namespace}'")
                except Exception as e:
                    logger.error(f"Failed to rollback from namespace '{namespace}': {e}")
                    failed_count += len(vector_ids)
            
            return {
                'rollback_count': success_count,
                'failed_count': failed_count,
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error during rollback: {e}")
            raise