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
from utils.metadata_processor import process_metadata_for_pinecone, extract_namespace_from_record

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
        vectors_by_namespace = {}  # Group vectors by namespace
        processed_record_ids = set()
        total_upserted_count = 0
        
        for i, record in enumerate(chunk_data):
            try:
                # Extract required fields
                record_id = record.get('id')
                formatted_text = record.get('formatted_text')
                
                # Extract namespace from record_type
                namespace = extract_namespace_from_record(record, 'record_type')
                
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
                metadata.pop('record_type', None)  # Used for namespace
                
                # Process metadata to remove commas and properly type fields
                metadata = process_metadata_for_pinecone(metadata, preserve_formatted_text=False)
                
                # Add processed text field (preserving commas in formatted_text)
                metadata['text'] = sanitized_text
                
                # Add processing metadata
                metadata.update({
                    'data_source': 's3_csv_step_functions',
                    'task_id': task_id,
                    'chunk_id': chunk_id,
                    's3_key': s3_key,
                    'question_id': str(question_id)
                })
                
                vector_data = {
                    'id': vector_id,
                    'values': embedding,
                    'metadata': metadata
                }
                
                # Group vectors by namespace
                if namespace not in vectors_by_namespace:
                    vectors_by_namespace[namespace] = []
                vectors_by_namespace[namespace].append(vector_data)
                processed_count += 1
                
                # Log progress for chunks
                if processed_count % 100 == 0:
                    logger.info(f"{chunk_id}: Processed {processed_count}/{actual_records} records")
                
                # Batch upsert every 500 vectors to manage memory
                # Check if any namespace has accumulated enough vectors
                should_upsert = False
                for ns, vectors in vectors_by_namespace.items():
                    if len(vectors) >= 500:
                        should_upsert = True
                        break
                
                if should_upsert:
                    # Upsert vectors for namespaces with >= 500 vectors
                    for ns, vectors in list(vectors_by_namespace.items()):
                        if len(vectors) >= 500:
                            batch_upserted = upsert_vectors_batch(pinecone_client, vectors, chunk_id, namespace=ns)
                            total_upserted_count += batch_upserted
                            vectors_by_namespace[ns] = []
                
            except Exception as e:
                logger.error(f"Error processing record {record_id} in {chunk_id}: {e}")
                failed_count += 1
        
        # Final batch upload if any vectors remain
        has_remaining = any(len(vectors) > 0 for vectors in vectors_by_namespace.values())
        if has_remaining:
            # Upsert remaining vectors for each namespace
            for ns, vectors in vectors_by_namespace.items():
                if vectors:
                    batch_upserted = upsert_vectors_batch(pinecone_client, vectors, chunk_id, namespace=ns)
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


def upsert_vectors_batch(pinecone_client: PineconeClient, vectors: List[Dict], chunk_id: str, namespace: str = None) -> int:
    """
    Upsert a batch of vectors to Pinecone with error handling.
    
    Args:
        pinecone_client: Pinecone client instance
        vectors: List of vector data to upsert
        chunk_id: Chunk identifier for logging
        namespace: Optional namespace to upsert vectors into
        
    Returns:
        int: Number of vectors successfully upserted
    """
    try:
        namespace_msg = f" to namespace '{namespace}'" if namespace else " to default namespace"
        logger.info(f"{chunk_id}: Upserting batch of {len(vectors)} vectors{namespace_msg}")
        
        pinecone_response = pinecone_client.upsert_vectors(vectors, namespace=namespace)
        batch_upserted_count = pinecone_response.get('upserted_count', 0)
        
        logger.info(f"{chunk_id}: Successfully upserted {batch_upserted_count} vectors{namespace_msg}")
        return batch_upserted_count
        
    except Exception as e:
        logger.error(f"{chunk_id}: Pinecone upsert failed for namespace '{namespace}': {e}")
        raise


def create_chunk_result(chunk_id: str, status: str, processed_records: int = 0, 
                       failed_records: int = 0, vectors_upserted: int = 0, 
                       actual_records: int = 0, error: str = None, message: str = None) -> Dict[str, Any]:
    """Create standardized chunk processing result."""
    # Minimal result to avoid Step Functions data size limits
    result = {
        "chunk_id": chunk_id,
        "status": status,
        "count": vectors_upserted  # Single count field instead of multiple
    }
    
    # Only add error for failed chunks (keep it short)
    if error and status == "FAILED":
        # Truncate error message to avoid data size issues
        result["error"] = error[:100] if len(error) > 100 else error
        
    return result