import json
import logging
import os
from typing import Dict, Any
from services.namespace_migrator import NamespaceMigrator
from services.dynamodb_client import DynamoDBClient

logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Step Functions handler for inventory phase of migration.
    Scans existing records to inventory record_types and estimates work.
    
    Args:
        event: Event containing migration parameters
        context: Lambda context
        
    Returns:
        Dict containing inventory results and chunk information
    """
    try:
        # Extract parameters
        task_id = event.get('task_id')
        source_namespace = event.get('source_namespace', '')
        namespace_mapping = event.get('namespace_mapping', {})
        batch_size = event.get('batch_size', 100)
        rate_limit = event.get('rate_limit', 10)
        dry_run = event.get('dry_run', False)
        resume_token = event.get('resume_token')
        
        logger.info(f"Starting inventory phase for task {task_id}")
        
        # Initialize clients
        dynamodb_client = DynamoDBClient()
        migrator = NamespaceMigrator(dry_run=dry_run)
        
        # Update environment variables
        os.environ['MIGRATION_BATCH_SIZE'] = str(batch_size)
        os.environ['MIGRATION_RATE_LIMIT'] = str(rate_limit)
        
        # Update task status
        dynamodb_client.update_task_metadata(task_id, {
            'migration_progress.phase': 'INVENTORY'
        })
        
        # Perform inventory
        inventory = migrator.inventory_record_types(
            namespace=source_namespace,
            sample_size=10000  # Larger sample for better accuracy
        )
        
        total_vectors = inventory.get('total_vectors', 0)
        record_types = inventory.get('record_types', {})
        
        # Calculate chunks for parallel processing
        chunk_size = batch_size * 10  # Process multiple batches per chunk
        total_chunks = (total_vectors + chunk_size - 1) // chunk_size
        
        chunks = []
        for i in range(total_chunks):
            start_offset = i * chunk_size
            end_offset = min((i + 1) * chunk_size, total_vectors)
            
            chunks.append({
                'chunk_id': i,
                'task_id': task_id,
                'source_namespace': source_namespace,
                'namespace_mapping': namespace_mapping,
                'start_offset': start_offset,
                'end_offset': end_offset,
                'batch_size': batch_size,
                'rate_limit': rate_limit,
                'dry_run': dry_run
            })
        
        # Handle resume token
        if resume_token:
            resume_data = migrator.parse_resume_token(resume_token)
            last_processed_id = resume_data.get('last_id')
            
            # Mark chunks before resume point as completed
            # This is simplified - in production would need more precise tracking
            logger.info(f"Resume token provided: {resume_token}")
        
        # Update DynamoDB with inventory results
        dynamodb_client.update_task_metadata(task_id, {
            'inventory': inventory,
            'total_records': total_vectors,
            'record_types_found': len(record_types),
            'migration_progress.total_chunks': total_chunks,
            'migration_progress.phase': 'READY_FOR_MIGRATION'
        })
        
        logger.info(f"Inventory complete: {total_vectors} vectors in {total_chunks} chunks")
        
        return {
            'success': True,
            'task_id': task_id,
            'total_vectors': total_vectors,
            'total_chunks': total_chunks,
            'chunks': chunks,
            'inventory': inventory,
            'source_namespace': source_namespace,
            'namespace_mapping': namespace_mapping
        }
        
    except Exception as e:
        logger.error(f"Error in inventory phase: {e}")
        
        # Update task with error
        if task_id:
            try:
                dynamodb_client = DynamoDBClient()
                dynamodb_client.set_task_error(task_id, f"Inventory failed: {str(e)}")
            except:
                pass
        
        return {
            'success': False,
            'error': str(e),
            'task_id': task_id
        }