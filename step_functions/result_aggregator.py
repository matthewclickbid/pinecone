import json
import logging
from typing import Dict, Any, List
from services.dynamodb_client import DynamoDBClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Result Aggregator Lambda for Step Functions workflow.
    Collects and aggregates results from all chunk processors.
    
    Args:
        event: Contains task_id, chunk_results, initialization data, or error info
        context: Lambda context
        
    Returns:
        Dict: Final aggregated results
    """
    try:
        task_id = event['task_id']
        logger.info(f"Aggregating results for task {task_id}")
        
        # Initialize DynamoDB client
        dynamodb_client = DynamoDBClient()
        
        # Handle different types of events
        if 'error_type' in event:
            # This is an error handling invocation
            return handle_error_aggregation(event, dynamodb_client)
        
        # Normal successful processing aggregation
        chunk_results = event.get('chunk_results', [])
        initialization = event.get('initialization', {})
        
        logger.info(f"Processing results from {len(chunk_results)} chunks")
        
        # Aggregate chunk results
        total_processed = 0
        total_failed = 0
        total_vectors_upserted = 0
        completed_chunks = 0
        failed_chunks = 0
        chunk_details = []
        
        for chunk_result in chunk_results:
            try:
                # Extract chunk data (handle nested Payload structure from Lambda invoke)
                if 'Payload' in chunk_result:
                    chunk_data = chunk_result['Payload']
                else:
                    chunk_data = chunk_result
                
                chunk_id = chunk_data.get('chunk_id', 'unknown')
                status = chunk_data.get('status', 'UNKNOWN')
                processed = chunk_data.get('processed_records', 0)
                failed = chunk_data.get('failed_records', 0)
                vectors_upserted = chunk_data.get('vectors_upserted', 0)
                
                total_processed += processed
                total_failed += failed
                total_vectors_upserted += vectors_upserted
                
                if status == 'COMPLETED':
                    completed_chunks += 1
                elif status == 'FAILED':
                    failed_chunks += 1
                
                chunk_details.append({
                    'chunk_id': chunk_id,
                    'status': status,
                    'processed_records': processed,
                    'failed_records': failed,
                    'vectors_upserted': vectors_upserted,
                    'error': chunk_data.get('error')
                })
                
                logger.info(f"Chunk {chunk_id}: {status}, processed={processed}, failed={failed}, vectors={vectors_upserted}")
                
            except Exception as e:
                logger.error(f"Error processing chunk result: {e}")
                failed_chunks += 1
                chunk_details.append({
                    'chunk_id': 'parse_error',
                    'status': 'FAILED',
                    'error': str(e)
                })
        
        # Determine overall task status
        total_chunks = len(chunk_results)
        if failed_chunks == 0:
            final_status = 'COMPLETED'
        elif completed_chunks == 0:
            final_status = 'FAILED'
        else:
            final_status = 'PARTIALLY_COMPLETED'
        
        # Calculate success rate
        success_rate = (completed_chunks / total_chunks * 100) if total_chunks > 0 else 0
        
        logger.info(f"Task {task_id} aggregation summary:")
        logger.info(f"  Total chunks: {total_chunks}")
        logger.info(f"  Completed chunks: {completed_chunks}")
        logger.info(f"  Failed chunks: {failed_chunks}")
        logger.info(f"  Success rate: {success_rate:.1f}%")
        logger.info(f"  Total records processed: {total_processed}")
        logger.info(f"  Total records failed: {total_failed}")
        logger.info(f"  Total vectors upserted: {total_vectors_upserted}")
        logger.info(f"  Final status: {final_status}")
        
        # Update final task status in DynamoDB
        try:
            dynamodb_client.update_task_status(
                task_id=task_id,
                status=final_status,
                total_records=total_processed + total_failed,
                processed_records=total_processed,
                failed_records=total_failed,
                total_upserted_to_pinecone=total_vectors_upserted,
                completed_chunks=completed_chunks,
                failed_chunks=failed_chunks,
                success_rate=success_rate
            )
            logger.info(f"Updated final task status to {final_status}")
            
        except Exception as e:
            logger.error(f"Failed to update final task status: {e}")
            # Don't fail the aggregation if DynamoDB update fails
        
        # Return aggregated results
        return {
            "success": True,
            "task_id": task_id,
            "final_status": final_status,
            "summary": {
                "total_chunks": total_chunks,
                "completed_chunks": completed_chunks,
                "failed_chunks": failed_chunks,
                "success_rate": success_rate,
                "total_records_processed": total_processed,
                "total_records_failed": total_failed,
                "total_vectors_upserted": total_vectors_upserted
            },
            "chunk_details": chunk_details,
            "initialization_info": initialization
        }
        
    except Exception as e:
        error_msg = f"Result aggregation failed for task {task_id}: {e}"
        logger.error(error_msg, exc_info=True)
        
        # Try to update task status to FAILED
        try:
            if 'task_id' in locals():
                dynamodb_client.set_task_error(task_id, error_msg)
        except:
            pass
        
        return {
            "success": False,
            "task_id": task_id if 'task_id' in locals() else 'unknown',
            "error": error_msg
        }


def handle_error_aggregation(event: Dict[str, Any], dynamodb_client: DynamoDBClient) -> Dict[str, Any]:
    """
    Handle error cases in the Step Functions workflow.
    
    Args:
        event: Error event data
        dynamodb_client: DynamoDB client instance
        
    Returns:
        Dict: Error handling result
    """
    try:
        task_id = event['task_id']
        error_type = event['error_type']
        error_details = event.get('error_details', {})
        
        logger.error(f"Handling {error_type} for task {task_id}")
        logger.error(f"Error details: {error_details}")
        
        # Map error types to readable messages
        error_messages = {
            'INITIALIZATION_FAILED': 'Failed to initialize CSV processing',
            'PROCESSING_FAILED': 'Failed during chunk processing',
            'AGGREGATION_FAILED': 'Failed to aggregate results'
        }
        
        error_message = error_messages.get(error_type, f"Unknown error: {error_type}")
        
        # Update task status to FAILED
        try:
            dynamodb_client.set_task_error(task_id, f"{error_message}: {error_details}")
            logger.info(f"Updated task {task_id} status to FAILED")
        except Exception as e:
            logger.error(f"Failed to update task error status: {e}")
        
        return {
            "success": False,
            "task_id": task_id,
            "error_type": error_type,
            "error_message": error_message,
            "error_details": error_details
        }
        
    except Exception as e:
        logger.error(f"Error in error handling: {e}")
        return {
            "success": False,
            "error": f"Error aggregation handler failed: {e}"
        }