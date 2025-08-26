import json
import logging
from typing import Dict, Any, List
from services.namespace_migrator import NamespaceMigrator
from services.dynamodb_client import DynamoDBClient

logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Step Functions handler for aggregating migration results and performing verification.
    
    Args:
        event: Event containing chunk results and migration parameters
        context: Lambda context
        
    Returns:
        Dict containing final migration results and verification
    """
    task_id = None
    
    try:
        # Extract parameters
        task_id = event.get('task_id')
        chunk_results = event.get('chunk_results', [])
        inventory = event.get('inventory', {})
        source_namespace = event.get('source_namespace', '')
        namespace_mapping = event.get('namespace_mapping', {})
        dry_run = event.get('dry_run', False)
        error_type = event.get('error_type')  # For error handling
        error_details = event.get('error_details')
        
        logger.info(f"Aggregating results for task {task_id}")
        
        # Initialize clients
        dynamodb_client = DynamoDBClient()
        
        # Handle error cases
        if error_type:
            logger.error(f"Migration failed with error type: {error_type}")
            dynamodb_client.update_task_status(task_id, 'FAILED')
            dynamodb_client.set_task_error(task_id, f"{error_type}: {error_details}")
            
            return {
                'task_id': task_id,
                'status': 'FAILED',
                'error_type': error_type,
                'error_details': error_details
            }
        
        # Aggregate chunk results
        total_processed = 0
        total_failed = 0
        failed_chunks = []
        successful_chunks = []
        
        for result in chunk_results:
            if isinstance(result, dict):
                # Handle successful chunk results
                if result.get('status') == 'SUCCESS' or 'vectors_processed' in result:
                    chunk_processed = result.get('vectors_processed', 0)
                    chunk_failed = result.get('vectors_failed', 0)
                    total_processed += chunk_processed
                    total_failed += chunk_failed
                    successful_chunks.append(result.get('chunk_id'))
                    
                    logger.info(f"Chunk {result.get('chunk_id')}: {chunk_processed} processed, {chunk_failed} failed")
                
                # Handle failed chunks
                elif result.get('status') == 'FAILED':
                    failed_chunks.append({
                        'chunk_id': result.get('chunk_id'),
                        'error': result.get('error')
                    })
                    logger.error(f"Chunk {result.get('chunk_id')} failed: {result.get('error')}")
        
        # Update task with aggregated results
        dynamodb_client.update_task_metadata(task_id, {
            'migration_progress.phase': 'VERIFICATION',
            'migration_progress.total_processed': total_processed,
            'migration_progress.total_failed': total_failed,
            'migration_progress.successful_chunks': len(successful_chunks),
            'migration_progress.failed_chunks': len(failed_chunks),
            'processed_records': total_processed,
            'failed_records': total_failed
        })
        
        # Perform verification if not in dry run mode
        verification_result = {}
        if not dry_run and total_processed > 0:
            try:
                logger.info("Starting migration verification")
                migrator = NamespaceMigrator(dry_run=False)
                
                target_namespaces = list(set(namespace_mapping.values()))
                verification_result = migrator.verify_migration(
                    source_namespace=source_namespace,
                    target_namespaces=target_namespaces,
                    sample_size=min(100, total_processed)
                )
                
                dynamodb_client.update_task_metadata(task_id, {
                    'verification_result': verification_result
                })
                
                logger.info(f"Verification complete: {verification_result.get('count_match', False)}")
            except Exception as e:
                logger.error(f"Verification failed: {e}")
                verification_result = {
                    'error': str(e),
                    'verification_skipped': True
                }
        
        # Determine final status
        if failed_chunks:
            final_status = 'PARTIALLY_FAILED'
        elif total_failed > 0:
            final_status = 'COMPLETED_WITH_ERRORS'
        else:
            final_status = 'COMPLETED'
        
        # Update final task status
        dynamodb_client.update_task_status(
            task_id,
            final_status,
            processed_records=total_processed,
            failed_records=total_failed
        )
        
        # Calculate success rate
        total_vectors = inventory.get('total_vectors', 0)
        success_rate = (total_processed / total_vectors * 100) if total_vectors > 0 else 0
        
        result = {
            'task_id': task_id,
            'status': final_status,
            'total_vectors': total_vectors,
            'vectors_processed': total_processed,
            'vectors_failed': total_failed,
            'success_rate': round(success_rate, 2),
            'successful_chunks': len(successful_chunks),
            'failed_chunks': failed_chunks,
            'verification': verification_result,
            'dry_run': dry_run
        }
        
        logger.info(f"Migration complete: {total_processed}/{total_vectors} vectors migrated ({success_rate:.2f}% success rate)")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in result aggregation: {e}")
        
        # Update task with error
        if task_id:
            try:
                dynamodb_client = DynamoDBClient()
                dynamodb_client.set_task_error(task_id, f"Aggregation failed: {str(e)}")
            except:
                pass
        
        return {
            'task_id': task_id,
            'status': 'FAILED',
            'error': str(e)
        }