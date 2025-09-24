"""
Data processing tasks for background execution.
Handles Metabase data processing, S3 CSV processing, and chunk-based processing.
"""

import logging
import traceback
import uuid
from typing import Dict, List, Any, Optional
from datetime import datetime
from celery import current_task
from celery.exceptions import Retry, WorkerLostError

from app.celery_config import celery_app
from app.config import settings
from app.services.dynamodb_client import DynamoDBClient
from app.services.metabase_client import MetabaseClient
from app.services.s3_csv_client import S3CSVClient
from app.services.openai_client import OpenAIClient
from app.services.pinecone_client import PineconeClient
from app.utils.text_sanitizer import sanitize_text
from app.utils.date_utils import parse_date_range


logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    name='app.tasks.data_processing.process_metabase_data',
    max_retries=3,
    default_retry_delay=300,  # 5 minutes
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=1800,  # 30 minutes
    retry_jitter=True
)
def process_metabase_data(
    self,
    task_id: str,
    start_date: str,
    end_date: str = None,
    question_id: str = None,
    namespace: str = None,
    batch_size: int = None
) -> Dict[str, Any]:
    """
    Process data from Metabase in the background.
    
    Args:
        task_id: DynamoDB task ID for tracking
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (optional)
        question_id: Metabase question ID (optional, uses default if not provided)
        namespace: Pinecone namespace (optional, uses default if not provided)
        batch_size: Batch size for processing (optional, uses default if not provided)
        
    Returns:
        Dict with processing results
    """
    
    # Initialize clients
    dynamodb = DynamoDBClient()
    metabase = MetabaseClient()
    openai_client = OpenAIClient()
    pinecone = PineconeClient()
    
    # Set default values
    end_date = end_date or start_date
    question_id = question_id or settings.METABASE_QUESTION_ID
    namespace = namespace or settings.PINECONE_NAMESPACE
    batch_size = batch_size or settings.BATCH_SIZE
    
    try:
        # Update task status to IN_PROGRESS
        dynamodb.update_task_status(task_id, 'IN_PROGRESS', started_at=datetime.utcnow().isoformat())
        logger.info(f"Started processing Metabase data for task {task_id}")
        
        # Parse date range
        start_date_parsed, end_date_parsed = parse_date_range(start_date, end_date)
        
        # Fetch data from Metabase
        logger.info(f"Fetching data from Metabase question {question_id} for dates {start_date} to {end_date}")
        data = metabase.get_question_data(
            question_id=int(question_id),
            start_date=start_date_parsed,
            end_date=end_date_parsed
        )
        
        if not data:
            logger.warning(f"No data found for date range {start_date} to {end_date}")
            dynamodb.update_task_status(
                task_id, 
                'COMPLETED', 
                total_records=0,
                processed_records=0,
                completed_at=datetime.utcnow().isoformat(),
                message="No data found for the specified date range"
            )
            return {
                'status': 'completed',
                'total_records': 0,
                'processed_records': 0,
                'message': 'No data found'
            }
        
        total_records = len(data)
        logger.info(f"Retrieved {total_records} records from Metabase")
        
        # Update task with total records
        dynamodb.update_task_status(task_id, 'IN_PROGRESS', total_records=total_records)
        
        # Process data in batches
        processed_records = 0
        failed_records = 0
        vectors_to_upsert = []
        
        for i, record in enumerate(data):
            try:
                # Update progress periodically
                if i % 100 == 0:
                    progress = (i / total_records) * 100
                    current_task.update_state(
                        state='PROGRESS',
                        meta={
                            'current': i,
                            'total': total_records,
                            'progress': progress,
                            'task_id': task_id
                        }
                    )
                    # Update DynamoDB with progress
                    dynamodb.increment_processed_records(task_id, min(100, i - processed_records))
                    processed_records = i
                
                # Extract and sanitize text content
                # For Metabase data, the entire record is the text content
                raw_text = record if isinstance(record, str) else str(record)
                text_content = sanitize_text(raw_text)
                if not text_content.strip():
                    logger.warning(f"Empty text content after sanitization for record {i}")
                    failed_records += 1
                    continue
                
                # Generate embedding
                try:
                    embedding = openai_client.get_embedding(text_content)
                except Exception as e:
                    logger.error(f"Failed to generate embedding for record {i}: {e}")
                    failed_records += 1
                    continue
                
                # Prepare vector for upsert
                vector_id = str(uuid.uuid4())
                vector_data = {
                    'id': vector_id,
                    'values': embedding,
                    'metadata': {
                        'text': text_content[:1000],  # Limit metadata size
                        'source': 'metabase',
                        'question_id': question_id,
                        'date_range': f"{start_date}_to_{end_date}",
                        'processed_at': datetime.utcnow().isoformat(),
                        'original_record_index': i
                    }
                }
                vectors_to_upsert.append(vector_data)
                
                # Upsert batch when batch_size is reached
                if len(vectors_to_upsert) >= batch_size:
                    try:
                        pinecone.upsert_vectors(vectors_to_upsert, namespace=namespace)
                        logger.debug(f"Upserted batch of {len(vectors_to_upsert)} vectors to namespace '{namespace}'")
                        vectors_to_upsert = []
                    except Exception as e:
                        logger.error(f"Failed to upsert batch: {e}")
                        failed_records += len(vectors_to_upsert)
                        vectors_to_upsert = []
                
            except Exception as e:
                logger.error(f"Error processing record {i}: {e}")
                failed_records += 1
                continue
        
        # Upsert any remaining vectors
        if vectors_to_upsert:
            try:
                pinecone.upsert_vectors(vectors_to_upsert, namespace=namespace)
                logger.debug(f"Upserted final batch of {len(vectors_to_upsert)} vectors to namespace '{namespace}'")
            except Exception as e:
                logger.error(f"Failed to upsert final batch: {e}")
                failed_records += len(vectors_to_upsert)
        
        # Calculate final counts
        processed_records = total_records - failed_records
        
        # Update final task status
        dynamodb.update_task_status(
            task_id,
            'COMPLETED',
            processed_records=processed_records,
            failed_records=failed_records,
            completed_at=datetime.utcnow().isoformat(),
            namespace_used=namespace,
            vectors_upserted=processed_records
        )
        
        result = {
            'status': 'completed',
            'task_id': task_id,
            'total_records': total_records,
            'processed_records': processed_records,
            'failed_records': failed_records,
            'namespace': namespace,
            'date_range': f"{start_date} to {end_date}"
        }
        
        logger.info(f"Completed processing task {task_id}: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Task {task_id} failed with error: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Update task status to failed
        try:
            dynamodb.set_task_error(task_id, str(e))
        except Exception as db_error:
            logger.error(f"Failed to update task error status: {db_error}")
        
        # Re-raise the exception to trigger retry logic
        raise


@celery_app.task(
    bind=True,
    name='app.tasks.data_processing.process_s3_csv_data',
    max_retries=2,
    default_retry_delay=600,  # 10 minutes
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=3600,  # 1 hour
    retry_jitter=True,
    time_limit=7200,  # 2 hour hard limit
    soft_time_limit=6900  # 1h 55m soft limit
)
def process_s3_csv_data(
    self,
    task_id: str,
    s3_key: str,
    question_id: str = None,
    namespace: str = None,
    chunk_size: int = None,
    use_enhanced_processor: bool = False,  # Changed to False to use parallel chunks
    streaming: bool = True
) -> Dict[str, Any]:
    """
    Process large CSV files from S3 using enhanced chunk-based processing.
    
    Args:
        task_id: DynamoDB task ID for tracking
        s3_key: S3 key path to the CSV file (or local file name if USE_LOCAL_FILES=true)
        question_id: Question ID for vector ID formatting (e.g., "10")
        namespace: Pinecone namespace (optional)
        chunk_size: Size of chunks for processing (optional)
        use_enhanced_processor: Whether to use the new enhanced processor
        streaming: Whether to use streaming mode for large files
        
    Returns:
        Dict with processing results
    """

    # Validate required parameters for CSV processing
    if not question_id:
        error_msg = "question_id is required for CSV processing to construct proper vector IDs (format: question_id.row_id)"
        logger.error(f"Task {task_id} failed: {error_msg}")

        # Update task error status
        dynamodb = DynamoDBClient()
        try:
            dynamodb.set_task_error(task_id, error_msg)
        except Exception as db_error:
            logger.error(f"Failed to update task error status: {db_error}")

        raise ValueError(error_msg)

    # Use enhanced processor if enabled
    if use_enhanced_processor:
        from app.workers.chunk_processor import process_csv_file
        
        try:
            logger.info(f"Using enhanced chunk processor for task {task_id}: {s3_key}")
            
            # Process with enhanced processor
            results = process_csv_file(
                task_id=task_id,
                file_path=s3_key,
                question_id=question_id,
                namespace=namespace,
                use_local=settings.USE_LOCAL_FILES,
                streaming=streaming,
                chunk_size=chunk_size,
                max_workers=settings.MAX_WORKERS
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Enhanced processor failed for task {task_id}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Update task error status
            dynamodb = DynamoDBClient()
            try:
                dynamodb.set_task_error(task_id, str(e))
            except Exception as db_error:
                logger.error(f"Failed to update task error status: {db_error}")
            
            raise
    
    # Fall back to original implementation
    # Initialize clients
    dynamodb = DynamoDBClient()
    s3_client = S3CSVClient()
    
    # Set default values
    namespace = namespace or settings.PINECONE_NAMESPACE
    chunk_size = chunk_size or settings.CHUNK_SIZE
    
    try:
        # Update task status
        dynamodb.update_task_status(task_id, 'IN_PROGRESS', started_at=datetime.utcnow().isoformat())
        logger.info(f"Started processing S3 CSV data for task {task_id}: {s3_key}")
        
        # Get CSV metadata and create chunks
        logger.info(f"Analyzing CSV file: {s3_key}")
        total_rows = s3_client.count_csv_rows(s3_key)
        
        if total_rows == 0:
            logger.warning(f"CSV file is empty: {s3_key}")
            dynamodb.update_task_status(
                task_id,
                'COMPLETED',
                total_records=0,
                processed_records=0,
                completed_at=datetime.utcnow().isoformat(),
                message="CSV file is empty"
            )
            return {
                'status': 'completed',
                'total_records': 0,
                'message': 'CSV file is empty'
            }
        
        # Calculate chunk information
        total_chunks = (total_rows + chunk_size - 1) // chunk_size
        logger.info(f"CSV has {total_rows} rows, will be processed in {total_chunks} chunks of {chunk_size}")
        
        # Create chunk metadata
        chunks_metadata = []
        for chunk_index in range(total_chunks):
            start_row = chunk_index * chunk_size
            end_row = min(start_row + chunk_size, total_rows)
            
            chunk_metadata = {
                'chunk_id': f"chunk_{chunk_index:04d}",
                'chunk_index': chunk_index,
                'start_row': start_row,
                'end_row': end_row,
                'expected_rows': end_row - start_row,
                's3_key': s3_key,
                'namespace': namespace,
                'task_id': task_id,
                'question_id': question_id
            }
            chunks_metadata.append(chunk_metadata)
        
        # Update task with chunk information
        dynamodb.update_task_with_chunks(
            task_id=task_id,
            total_chunks=total_chunks,
            estimated_total_records=total_rows,
            chunks_metadata=chunks_metadata
        )
        
        # Process chunks in parallel using Celery
        chunk_jobs = []
        for chunk_metadata in chunks_metadata:
            job = process_csv_chunk.delay(
                task_id=task_id,
                chunk_metadata=chunk_metadata
            )
            chunk_jobs.append({
                'job_id': job.id,
                'chunk_id': chunk_metadata['chunk_id'],
                'chunk_metadata': chunk_metadata
            })
        
        logger.info(f"Dispatched {len(chunk_jobs)} chunk processing jobs for task {task_id}")
        
        # Start aggregation task to monitor chunk completion
        aggregate_chunk_results.delay(
            task_id=task_id,
            chunk_jobs=chunk_jobs,
            expected_chunks=total_chunks
        )
        
        # Update task status to indicate chunk processing has started
        dynamodb.update_task_status(
            task_id,
            'PROCESSING_CHUNKS',
            message=f"Processing {total_chunks} chunks",
            chunks_dispatched=len(chunk_jobs)
        )
        
        return {
            'status': 'processing_chunks',
            'task_id': task_id,
            'total_chunks': total_chunks,
            'estimated_total_records': total_rows,
            'chunks_dispatched': len(chunk_jobs),
            'chunk_jobs': [{'job_id': job['job_id'], 'chunk_id': job['chunk_id']} for job in chunk_jobs]
        }
        
    except Exception as e:
        logger.error(f"Task {task_id} failed with error: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        try:
            dynamodb.set_task_error(task_id, str(e))
        except Exception as db_error:
            logger.error(f"Failed to update task error status: {db_error}")
        
        raise


@celery_app.task(
    bind=True,
    name='app.tasks.data_processing.process_csv_chunk',
    max_retries=3,
    default_retry_delay=120,  # 2 minutes
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,  # 10 minutes
    retry_jitter=True
)
def process_csv_chunk(self, task_id: str, chunk_metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single CSV chunk.
    
    Args:
        task_id: DynamoDB task ID for tracking
        chunk_metadata: Metadata about the chunk to process
        
    Returns:
        Dict with chunk processing results
    """
    
    chunk_id = chunk_metadata['chunk_id']
    
    # Initialize clients
    dynamodb = DynamoDBClient()
    s3_client = S3CSVClient()
    openai_client = OpenAIClient()
    pinecone = PineconeClient()
    
    try:
        logger.info(f"Processing chunk {chunk_id} for task {task_id}")
        
        # Update chunk status to IN_PROGRESS
        dynamodb.update_chunk_status(task_id, chunk_id, 'IN_PROGRESS')
        
        # Read CSV chunk
        chunk_data = s3_client.fetch_csv_chunk_by_rows(
            s3_key=chunk_metadata['s3_key'],
            start_row=chunk_metadata['start_row'] + 1,  # fetch_csv_chunk_by_rows uses 1-based indexing
            end_row=chunk_metadata['end_row']
        )
        
        if not chunk_data:
            logger.warning(f"No data in chunk {chunk_id}")
            dynamodb.update_chunk_status(
                task_id, chunk_id, 'COMPLETED',
                processed_records=0, failed_records=0, vectors_upserted=0
            )
            return {
                'status': 'completed',
                'chunk_id': chunk_id,
                'processed_records': 0,
                'message': 'No data in chunk'
            }
        
        # Process chunk data
        processed_records = 0
        failed_records = 0
        vectors_to_upsert = []
        
        for i, record in enumerate(chunk_data):
            try:
                # Update progress for large chunks
                if i % 100 == 0 and i > 0:
                    current_task.update_state(
                        state='PROGRESS',
                        meta={
                            'chunk_id': chunk_id,
                            'current': i,
                            'total': len(chunk_data),
                            'progress': (i / len(chunk_data)) * 100,
                            'task_id': task_id
                        }
                    )
                
                # Extract and sanitize text content
                # For CSV data, the text is in the 'formatted_text' field
                raw_text = record.get('formatted_text', '') if isinstance(record, dict) else str(record)
                text_content = sanitize_text(raw_text)
                if not text_content.strip():
                    failed_records += 1
                    continue
                
                # Generate embedding
                try:
                    embedding = openai_client.get_embedding(text_content)
                except Exception as e:
                    logger.warning(f"Failed to generate embedding for chunk {chunk_id}, record {i}: {e}")
                    failed_records += 1
                    continue
                
                # Get row ID from record
                if isinstance(record, dict):
                    row_id = record.get('id', '')
                    if not row_id:
                        raise ValueError(f"CSV record missing 'id' field required for vector ID construction at index {i}")
                else:
                    raise ValueError(f"Record at index {i} is not a dictionary, cannot extract 'id' field")

                # Get question_id from chunk metadata
                question_id = chunk_metadata.get('question_id')
                if not question_id:
                    raise ValueError(f"question_id is required for vector ID construction but was not provided in chunk metadata")

                # Prepare vector for upsert with proper ID format
                vector_id = f"{question_id}.{row_id}"
                vector_data = {
                    'id': vector_id,
                    'values': embedding,
                    'metadata': {
                        'text': text_content[:1000],  # Limit metadata size
                        'source': 's3_csv',
                        's3_key': chunk_metadata['s3_key'],
                        'chunk_id': chunk_id,
                        'record_index': i,
                        'task_id': task_id,
                        'question_id': question_id,
                        'row_id': row_id,
                        'processed_at': datetime.utcnow().isoformat()
                    }
                }
                vectors_to_upsert.append(vector_data)
                
                # Upsert in batches
                if len(vectors_to_upsert) >= settings.BATCH_SIZE:
                    try:
                        pinecone.upsert_vectors(vectors_to_upsert, namespace=chunk_metadata['namespace'])
                        processed_records += len(vectors_to_upsert)
                        vectors_to_upsert = []
                    except Exception as e:
                        logger.error(f"Failed to upsert batch in chunk {chunk_id}: {e}")
                        failed_records += len(vectors_to_upsert)
                        vectors_to_upsert = []
                
            except Exception as e:
                logger.error(f"Error processing record {i} in chunk {chunk_id}: {e}")
                failed_records += 1
                continue
        
        # Upsert any remaining vectors
        if vectors_to_upsert:
            try:
                pinecone.upsert_vectors(vectors_to_upsert, namespace=chunk_metadata['namespace'])
                processed_records += len(vectors_to_upsert)
            except Exception as e:
                logger.error(f"Failed to upsert final batch in chunk {chunk_id}: {e}")
                failed_records += len(vectors_to_upsert)
        
        # Update chunk status to completed
        dynamodb.update_chunk_status(
            task_id, chunk_id, 'COMPLETED',
            processed_records=processed_records,
            failed_records=failed_records,
            vectors_upserted=processed_records
        )
        
        result = {
            'status': 'completed',
            'chunk_id': chunk_id,
            'task_id': task_id,
            'processed_records': processed_records,
            'failed_records': failed_records,
            'total_records': len(chunk_data)
        }
        
        logger.info(f"Completed processing chunk {chunk_id}: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Chunk {chunk_id} failed with error: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Update chunk status to failed
        try:
            dynamodb.update_chunk_status(
                task_id, chunk_id, 'FAILED',
                error_message=str(e)
            )
        except Exception as db_error:
            logger.error(f"Failed to update chunk error status: {db_error}")
        
        raise


@celery_app.task(
    bind=True,
    name='app.tasks.data_processing.aggregate_chunk_results',
    max_retries=1,
    default_retry_delay=300,  # 5 minutes
)
def aggregate_chunk_results(
    self,
    task_id: str,
    chunk_jobs: List[Dict[str, Any]],
    expected_chunks: int,
    check_interval: int = 30  # seconds
) -> Dict[str, Any]:
    """
    Monitor chunk processing jobs and aggregate final results.
    
    Args:
        task_id: DynamoDB task ID
        chunk_jobs: List of chunk job information
        expected_chunks: Expected number of chunks
        check_interval: Interval to check job status (seconds)
        
    Returns:
        Dict with final aggregated results
    """
    
    import time
    from celery.result import AsyncResult
    
    dynamodb = DynamoDBClient()
    
    try:
        logger.info(f"Starting result aggregation for task {task_id}, monitoring {len(chunk_jobs)} jobs")
        
        completed_chunks = 0
        failed_chunks = 0
        max_wait_time = settings.TASK_TIMEOUT - 300  # Leave 5 minutes buffer
        start_time = time.time()
        
        while completed_chunks + failed_chunks < expected_chunks:
            # Check if we've exceeded max wait time
            if time.time() - start_time > max_wait_time:
                logger.warning(f"Timeout reached while waiting for chunks to complete for task {task_id}")
                break
            
            # Check status of all jobs
            still_running = 0
            for job_info in chunk_jobs:
                job_id = job_info['job_id']
                chunk_id = job_info['chunk_id']
                
                try:
                    result = AsyncResult(job_id, app=celery_app)
                    
                    if result.successful():
                        if job_info.get('status') != 'completed':
                            job_info['status'] = 'completed'
                            completed_chunks += 1
                            logger.debug(f"Chunk {chunk_id} completed successfully")
                    
                    elif result.failed():
                        if job_info.get('status') != 'failed':
                            job_info['status'] = 'failed'
                            failed_chunks += 1
                            logger.warning(f"Chunk {chunk_id} failed: {result.info}")
                    
                    else:
                        # Job is still running or pending
                        still_running += 1
                
                except Exception as e:
                    logger.error(f"Error checking status of job {job_id}: {e}")
                    if job_info.get('status') != 'failed':
                        job_info['status'] = 'failed'
                        failed_chunks += 1
            
            # Update progress
            progress = ((completed_chunks + failed_chunks) / expected_chunks) * 100
            current_task.update_state(
                state='PROGRESS',
                meta={
                    'task_id': task_id,
                    'completed_chunks': completed_chunks,
                    'failed_chunks': failed_chunks,
                    'still_running': still_running,
                    'progress': progress
                }
            )
            
            # Wait before next check if not all chunks are done
            if completed_chunks + failed_chunks < expected_chunks:
                time.sleep(check_interval)
        
        # Get final task status from DynamoDB to aggregate chunk results
        task_details = dynamodb.get_task(task_id)
        if not task_details:
            raise Exception(f"Task {task_id} not found in DynamoDB")
        
        # Calculate totals from chunk status
        total_processed = 0
        total_failed = 0
        total_vectors = 0
        
        chunk_processed = task_details.get('chunk_processed', {})
        chunk_failed = task_details.get('chunk_failed', {})
        chunk_vectors = task_details.get('chunk_vectors', {})
        
        for chunk_id in chunk_processed:
            total_processed += chunk_processed.get(chunk_id, 0)
        
        for chunk_id in chunk_failed:
            total_failed += chunk_failed.get(chunk_id, 0)
        
        for chunk_id in chunk_vectors:
            total_vectors += chunk_vectors.get(chunk_id, 0)
        
        # Update final task status
        final_status = 'COMPLETED' if failed_chunks == 0 else 'PARTIALLY_COMPLETED'
        
        dynamodb.update_task_status(
            task_id,
            final_status,
            processed_records=total_processed,
            failed_records=total_failed,
            completed_chunks=completed_chunks,
            failed_chunks=failed_chunks,
            vectors_upserted=total_vectors,
            completed_at=datetime.utcnow().isoformat()
        )
        
        result = {
            'status': final_status.lower(),
            'task_id': task_id,
            'total_chunks': expected_chunks,
            'completed_chunks': completed_chunks,
            'failed_chunks': failed_chunks,
            'total_processed': total_processed,
            'total_failed': total_failed,
            'vectors_upserted': total_vectors
        }
        
        logger.info(f"Completed result aggregation for task {task_id}: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Result aggregation failed for task {task_id}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        try:
            dynamodb.set_task_error(task_id, f"Result aggregation failed: {str(e)}")
        except Exception as db_error:
            logger.error(f"Failed to update task error status: {db_error}")
        
        raise