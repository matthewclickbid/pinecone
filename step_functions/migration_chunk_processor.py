import json
import logging
import os
import time
from typing import Dict, Any, List
from services.namespace_migrator import NamespaceMigrator
from services.dynamodb_client import DynamoDBClient

logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Step Functions handler for processing a single chunk of vectors during migration.
    
    Args:
        event: Event containing chunk parameters
        context: Lambda context
        
    Returns:
        Dict containing processing results for this chunk
    """
    chunk_id = None
    task_id = None
    
    try:
        # Extract parameters
        chunk_id = event.get('chunk_id')
        task_id = event.get('task_id')
        source_namespace = event.get('source_namespace', '')
        namespace_mapping = event.get('namespace_mapping', {})
        start_offset = event.get('start_offset', 0)
        end_offset = event.get('end_offset')
        batch_size = event.get('batch_size', 100)
        rate_limit = event.get('rate_limit', 10)
        dry_run = event.get('dry_run', False)
        
        logger.info(f"Processing chunk {chunk_id} for task {task_id} (offset {start_offset}-{end_offset})")
        
        # Initialize clients
        dynamodb_client = DynamoDBClient()
        migrator = NamespaceMigrator(dry_run=dry_run)
        
        # Update environment variables
        os.environ['MIGRATION_BATCH_SIZE'] = str(batch_size)
        os.environ['MIGRATION_RATE_LIMIT'] = str(rate_limit)
        
        # Update chunk status
        dynamodb_client.update_task_metadata(task_id, {
            f'chunk_{chunk_id}_status': 'IN_PROGRESS',
            f'chunk_{chunk_id}_start_time': time.time()
        })
        
        # Process vectors in this chunk
        vectors_processed = 0
        vectors_failed = 0
        rollback_data = []
        next_page_token = None
        vectors_to_process = end_offset - start_offset
        current_offset = 0
        
        # Skip to start_offset using pagination
        while current_offset < start_offset:
            list_kwargs = {
                'namespace': source_namespace,
                'limit': min(batch_size, start_offset - current_offset)
            }
            if next_page_token:
                list_kwargs['pagination_token'] = next_page_token
                
            skip_response = migrator.index.list_paginated(**list_kwargs)
            vector_list = skip_response.vectors if hasattr(skip_response, 'vectors') else []
            vector_ids = [v.id if hasattr(v, 'id') else str(v) for v in vector_list]
            current_offset += len(vector_ids)
            next_page_token = getattr(skip_response, 'pagination_token', None)
            if not next_page_token:
                break
        
        # Process vectors from start_offset to end_offset
        while vectors_processed < vectors_to_process:
            # Check Lambda timeout (leave 30 seconds for cleanup)
            if context.get_remaining_time_in_millis() < 30000:
                logger.warning(f"Lambda timeout approaching for chunk {chunk_id}")
                break
            
            # List vectors with pagination
            list_kwargs = {
                'namespace': source_namespace,
                'limit': min(batch_size, vectors_to_process - vectors_processed)
            }
            if next_page_token:
                list_kwargs['pagination_token'] = next_page_token
                
            list_response = migrator.index.list_paginated(**list_kwargs)
            
            # Extract vector list and convert to IDs
            vector_list = list_response.vectors if hasattr(list_response, 'vectors') else []
            if not vector_list:
                break
            
            # Extract actual ID strings from vector objects
            vector_ids = [v.id if hasattr(v, 'id') else str(v) for v in vector_list]
            
            # Migrate batch
            success, failed, batch_rollback = migrator.migrate_batch(
                vector_ids=vector_ids,
                source_namespace=source_namespace,
                target_namespace_map=namespace_mapping,
                rollback_data=rollback_data
            )
            
            vectors_processed += success
            vectors_failed += failed
            
            # Update progress periodically
            if vectors_processed % (batch_size * 5) == 0:
                dynamodb_client.increment_processed_records(task_id, success)
                if failed > 0:
                    dynamodb_client.increment_failed_records(task_id, failed)
            
            next_page_token = getattr(list_response, 'pagination_token', None)
            if not next_page_token:
                break
            
            # Log progress
            if vectors_processed % 1000 == 0:
                logger.info(f"Chunk {chunk_id}: {vectors_processed}/{vectors_to_process} vectors processed")
        
        # Final update for this chunk
        dynamodb_client.increment_processed_records(task_id, vectors_processed)
        if vectors_failed > 0:
            dynamodb_client.increment_failed_records(task_id, vectors_failed)
        
        # Update chunk status
        dynamodb_client.update_task_metadata(task_id, {
            f'chunk_{chunk_id}_status': 'COMPLETED',
            f'chunk_{chunk_id}_processed': vectors_processed,
            f'chunk_{chunk_id}_failed': vectors_failed,
            f'chunk_{chunk_id}_end_time': time.time()
        })
        
        logger.info(f"Chunk {chunk_id} complete: {vectors_processed} processed, {vectors_failed} failed")
        
        return {
            'chunk_id': chunk_id,
            'task_id': task_id,
            'status': 'SUCCESS',
            'vectors_processed': vectors_processed,
            'vectors_failed': vectors_failed,
            'rollback_data_count': len(rollback_data)
        }
        
    except Exception as e:
        logger.error(f"Error processing chunk {chunk_id}: {e}")
        
        # Update chunk status with error
        if task_id and chunk_id is not None:
            try:
                dynamodb_client = DynamoDBClient()
                dynamodb_client.update_task_metadata(task_id, {
                    f'chunk_{chunk_id}_status': 'FAILED',
                    f'chunk_{chunk_id}_error': str(e),
                    f'chunk_{chunk_id}_end_time': time.time()
                })
            except:
                pass
        
        return {
            'chunk_id': chunk_id,
            'task_id': task_id,
            'status': 'FAILED',
            'error': str(e)
        }