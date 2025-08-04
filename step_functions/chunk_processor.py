import json
import logging
import os
from typing import Dict, Any, List
import boto3

from services.s3_csv_client import S3CSVClient
from services.openai_client import OpenAIClient
from services.pinecone_client import PineconeClient
from services.dynamodb_client import DynamoDBClient
from utils.text_sanitizer import sanitize_text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Chunk Processor Lambda for Step Functions workflow.
    Processes a specific chunk of CSV records.
    
    Args:
        event: Contains task_id, chunk_id, s3_key, start_row, end_row, question_id, bucket_name
        context: Lambda context
        
    Returns:
        Dict: Processing results for this chunk
    """
    try:
        # Extract parameters
        task_id = event['task_id']
        chunk_id = event['chunk_id']
        s3_key = event['s3_key']
        start_row = event['start_row']
        end_row = event['end_row']
        question_id = event['question_id']
        bucket_name = event['bucket_name']
        
        logger.info(f"Processing {chunk_id} for task {task_id}")
        logger.info(f"Rows {start_row}-{end_row} from s3://{bucket_name}/{s3_key}")
        logger.info(f"Question ID: {question_id}")
        
        # Initialize clients
        s3_csv_client = S3CSVClient(bucket_name=bucket_name, chunk_size=1000)
        openai_client = OpenAIClient(rate_limit_per_second=20)
        pinecone_client = PineconeClient()
        dynamodb_client = DynamoDBClient()
        
        # Update chunk status to IN_PROGRESS
        try:
            dynamodb_client.update_chunk_status(task_id, chunk_id, 'IN_PROGRESS')
            logger.info(f"Updated {chunk_id} status to IN_PROGRESS")
        except Exception as e:
            logger.warning(f"Could not update chunk status: {e}")
        
        # Fetch chunk data from S3
        try:
            chunk_data = s3_csv_client.fetch_csv_chunk_by_rows(s3_key, start_row, end_row)
            actual_records = len(chunk_data)
            
            if not chunk_data:
                logger.warning(f"No data found for {chunk_id} in rows {start_row}-{end_row}")
                return create_chunk_result(
                    chunk_id=chunk_id,
                    status='COMPLETED',
                    processed_records=0,
                    failed_records=0,
                    vectors_upserted=0,
                    message="No data in chunk range"
                )
                
            logger.info(f"Retrieved {actual_records} records for {chunk_id}")
            
        except Exception as e:
            error_msg = f"Failed to fetch chunk data: {e}"
            logger.error(error_msg)
            dynamodb_client.update_chunk_status(task_id, chunk_id, 'FAILED', error_msg)
            return create_chunk_result(
                chunk_id=chunk_id,
                status='FAILED',
                error=error_msg
            )
        
        # Process records in this chunk
        processed_count = 0
        failed_count = 0
        vectors_to_upsert = []
        processed_record_ids = set()
        total_upserted_count = 0
        
        for i, record in enumerate(chunk_data):
            try:
                # Extract required fields
                record_id = record.get('id')
                formatted_text = record.get('formatted_text')
                
                # Validate required fields
                if not all([record_id, formatted_text]):
                    logger.warning(f"Record missing required fields: id={record_id}, has_text={bool(formatted_text)}")
                    failed_count += 1
                    continue
                
                # Check for duplicate record IDs within chunk
                if record_id in processed_record_ids:
                    logger.warning(f"DUPLICATE DETECTED in {chunk_id}: Record ID {record_id} already processed - skipping")
                    failed_count += 1
                    continue
                
                processed_record_ids.add(record_id)
                
                # Sanitize text
                sanitized_text = sanitize_text(formatted_text)
                
                if not sanitized_text:
                    logger.warning(f"Record {record_id} has empty text after sanitization")
                    failed_count += 1
                    continue
                
                # Generate embedding
                embedding = openai_client.get_embedding(sanitized_text)
                
                # Create vector ID with question_id prefix
                vector_id = f"{question_id}.{record_id}"
                
                # Start with all source record fields
                metadata = dict(record)
                
                # Remove fields that have special handling
                metadata.pop('id', None)  # Used for vector ID
                metadata.pop('formatted_text', None)  # Becomes 'text'
                
                # Add processed text field
                metadata['text'] = sanitized_text
                
                # Add processing metadata
                metadata.update({
                    'data_source': 's3_csv_step_functions',
                    'task_id': task_id,
                    'chunk_id': chunk_id,
                    's3_key': s3_key,
                    'question_id': str(question_id)
                })
                
                # Convert all values to strings for Pinecone compatibility
                metadata = {k: str(v) for k, v in metadata.items()}
                
                vector_data = {
                    'id': vector_id,
                    'values': embedding,
                    'metadata': metadata
                }
                
                vectors_to_upsert.append(vector_data)
                processed_count += 1
                
                # Log progress for chunks
                if processed_count % 100 == 0:
                    logger.info(f"{chunk_id}: Processed {processed_count}/{actual_records} records")
                
                # Batch upsert every 500 vectors to manage memory
                if len(vectors_to_upsert) >= 500:
                    batch_upserted = upsert_vectors_batch(pinecone_client, vectors_to_upsert, chunk_id)
                    total_upserted_count += batch_upserted
                    vectors_to_upsert = []
                
            except Exception as e:
                logger.error(f"Error processing record {record_id} in {chunk_id}: {e}")
                failed_count += 1
        
        # Final batch upload if any vectors remain
        if vectors_to_upsert:
            batch_upserted = upsert_vectors_batch(pinecone_client, vectors_to_upsert, chunk_id)
            total_upserted_count += batch_upserted
        
        # Update chunk completion status
        try:
            dynamodb_client.update_chunk_status(
                task_id, 
                chunk_id, 
                'COMPLETED',
                processed_records=processed_count,
                failed_records=failed_count,
                vectors_upserted=total_upserted_count
            )
        except Exception as e:
            logger.warning(f"Could not update final chunk status: {e}")
        
        logger.info(f"{chunk_id} completed: {processed_count} processed, {failed_count} failed, {total_upserted_count} vectors upserted")
        
        return create_chunk_result(
            chunk_id=chunk_id,
            status='COMPLETED',
            processed_records=processed_count,
            failed_records=failed_count,
            vectors_upserted=total_upserted_count,
            actual_records=actual_records
        )
        
    except Exception as e:
        error_msg = f"Chunk processor failed for {chunk_id}: {e}"
        logger.error(error_msg, exc_info=True)
        
        # Try to update chunk status if possible
        try:
            if 'task_id' in locals() and 'chunk_id' in locals():
                dynamodb_client.update_chunk_status(task_id, chunk_id, 'FAILED', error_msg)
        except:
            pass
            
        return create_chunk_result(
            chunk_id=chunk_id if 'chunk_id' in locals() else 'unknown',
            status='FAILED',
            error=error_msg
        )


def upsert_vectors_batch(pinecone_client: PineconeClient, vectors: List[Dict], chunk_id: str) -> int:
    """
    Upsert a batch of vectors to Pinecone with error handling.
    
    Args:
        pinecone_client: Pinecone client instance
        vectors: List of vector data to upsert
        chunk_id: Chunk identifier for logging
        
    Returns:
        int: Number of vectors successfully upserted
    """
    try:
        logger.info(f"{chunk_id}: Upserting batch of {len(vectors)} vectors to Pinecone")
        
        pinecone_response = pinecone_client.upsert_vectors(vectors)
        batch_upserted_count = pinecone_response.get('upserted_count', 0)
        
        logger.info(f"{chunk_id}: Successfully upserted {batch_upserted_count} vectors")
        return batch_upserted_count
        
    except Exception as e:
        logger.error(f"{chunk_id}: Pinecone upsert failed: {e}")
        raise


def create_chunk_result(chunk_id: str, status: str, processed_records: int = 0, 
                       failed_records: int = 0, vectors_upserted: int = 0, 
                       actual_records: int = 0, error: str = None, message: str = None) -> Dict[str, Any]:
    """Create standardized chunk processing result."""
    result = {
        "chunk_id": chunk_id,
        "status": status,
        "processed_records": processed_records,
        "failed_records": failed_records,
        "vectors_upserted": vectors_upserted,
        "actual_records": actual_records
    }
    
    if error:
        result["error"] = error
    if message:
        result["message"] = message
        
    return result