import json
import logging
import os
from typing import Dict, Any

# Import all required services
from services.metabase_client import MetabaseClient
from services.s3_csv_client import S3CSVClient
from services.openai_client import OpenAIClient
from services.pinecone_client import PineconeClient
from services.dynamodb_client import DynamoDBClient
from utils.text_sanitizer import sanitize_text
from utils.metadata_processor import process_metadata_for_pinecone, extract_namespace_from_record

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def async_process_handler(event, context):
    """
    Handler for async processing of Metabase data.
    """
    try:
        task_id = event.get('task_id')
        start_date = event.get('start_date')
        end_date = event.get('end_date')
        question_id = event.get('question_id')  # Optional parameter
        data_source = event.get('data_source', 'metabase')  # Default to metabase
        s3_key = event.get('s3_key')  # S3 object key for CSV files
        date_column = event.get('date_column', 'created_at')  # Column for date filtering
        lambda_request_id = context.aws_request_id
        
        logger.warning(f"Starting async processing for task {task_id} (request: {lambda_request_id})")
        logger.warning(f"Data source: {data_source}")
        
        if data_source == 'metabase':
            if question_id:
                logger.warning(f"Using custom question_id: {question_id}")
            else:
                logger.warning("Using default question_id from environment variable")
        elif data_source == 's3_csv':
            logger.warning(f"Using S3 CSV file: {s3_key}")
            logger.warning(f"Question ID for vector IDs: {question_id}")
            if not s3_key:
                raise ValueError("s3_key is required when data_source is 's3_csv'")
            if not question_id:
                raise ValueError("question_id is required when data_source is 's3_csv' for vector ID generation")
        else:
            raise ValueError(f"Invalid data_source: {data_source}. Must be 'metabase' or 's3_csv'")
        
        # Initialize DynamoDB client first to check for locks
        try:
            dynamodb_client = DynamoDBClient()
            logger.warning("DynamoDBClient initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing DynamoDBClient: {e}")
            raise

        # Try to acquire processing lock for this date range (include data source info for uniqueness)
        if data_source == 'metabase':
            lock_key_suffix = f"_q{question_id}" if question_id else ""
        else:  # s3_csv
            # Use s3_key hash for uniqueness since it could be long
            import hashlib
            s3_hash = hashlib.md5(s3_key.encode()).hexdigest()[:8]
            lock_key_suffix = f"_s3_{s3_hash}"
        
        lock_acquired = dynamodb_client.acquire_processing_lock(f"{start_date}{lock_key_suffix}", f"{end_date}{lock_key_suffix}", lambda_request_id)
        if not lock_acquired:
            logger.warning(f"Another Lambda is already processing {start_date} to {end_date} with data_source {data_source}. Exiting gracefully.")
            dynamodb_client.update_task_status(task_id, 'SKIPPED', 
                                             skip_reason=f"Already being processed by another Lambda")
            return
        
        logger.warning(f"Acquired processing lock for {start_date} to {end_date} (data_source: {data_source})")
        
        # Initialize data source clients
        metabase_client = None
        s3_csv_client = None
        
        if data_source == 'metabase':
            try:
                metabase_client = MetabaseClient(question_id=question_id)
                logger.warning("MetabaseClient initialized successfully")
            except Exception as e:
                logger.error(f"Error initializing MetabaseClient: {e}")
                dynamodb_client.release_processing_lock(f"{start_date}{lock_key_suffix}", f"{end_date}{lock_key_suffix}", lambda_request_id)
                raise
        else:  # s3_csv
            try:
                s3_csv_client = S3CSVClient(chunk_size=1000)  # Process 1000 rows at a time
                logger.warning("S3CSVClient initialized successfully")
            except Exception as e:
                logger.error(f"Error initializing S3CSVClient: {e}")
                dynamodb_client.release_processing_lock(f"{start_date}{lock_key_suffix}", f"{end_date}{lock_key_suffix}", lambda_request_id)
                raise
            
        try:
            openai_client = OpenAIClient(rate_limit_per_second=20)
            logger.warning("OpenAIClient initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing OpenAIClient: {e}")
            dynamodb_client.release_processing_lock(f"{start_date}{lock_key_suffix}", f"{end_date}{lock_key_suffix}", lambda_request_id)
            raise
        
        try:
            # Update task status to IN_PROGRESS
            dynamodb_client.update_task_status(task_id, 'IN_PROGRESS')
            
            # Fetch data based on data source
            if data_source == 'metabase':
                logger.warning("Fetching data from Metabase...")
                source_data = metabase_client.fetch_question_results(start_date, end_date)
                
                if not source_data:
                    logger.warning("No data returned from Metabase")
                    dynamodb_client.update_task_status(task_id, 'COMPLETED', total_records=0)
                    return
            else:  # s3_csv
                logger.warning(f"Processing large CSV from S3 in chunks: {s3_key}")
                # For S3 CSV, we process in chunks to handle large files (100MB+)
                # First, get a sample to estimate total records and validate structure
                try:
                    validation_result = s3_csv_client.validate_csv_structure(s3_key, ['id', 'formatted_text'])
                    logger.warning(f"CSV validation result: {validation_result}")
                    
                    if not validation_result['valid']:
                        error_msg = f"CSV validation failed: missing columns {validation_result['missing_columns']}"
                        logger.error(error_msg)
                        dynamodb_client.set_task_error(task_id, error_msg)
                        return
                        
                    file_size_mb = validation_result['file_size'] / (1024 * 1024)
                    logger.warning(f"Processing CSV file: {file_size_mb:.1f} MB")
                    
                except Exception as e:
                    error_msg = f"CSV validation error: {e}"
                    logger.error(error_msg)
                    dynamodb_client.set_task_error(task_id, error_msg)
                    return
                
                # Process CSV in chunks to handle large files efficiently
                processed_count = 0
                failed_count = 0
                vectors_by_namespace = {}  # Group vectors by namespace
                processed_record_ids = set()
                total_upserted_count = 0
                chunk_count = 0
                
                logger.warning("Starting chunked processing of large CSV file...")
                
                try:
                    # Process CSV in chunks - no date filtering for S3 CSV
                    for chunk in s3_csv_client.fetch_csv_data_no_filter(s3_key):
                        chunk_count += 1
                        chunk_size = len(chunk)
                        logger.warning(f"Processing chunk {chunk_count} with {chunk_size} records")
                        
                        for i, record in enumerate(chunk):
                            try:
                                # Extract required fields
                                record_id = record.get('id')
                                event_id = record.get('event_id')
                                org_id = record.get('org_id')
                                formatted_text = record.get('formatted_text')
                                
                                # Extract namespace from record_type
                                namespace = extract_namespace_from_record(record, 'record_type')
                                
                                # Validate required fields
                                if not all([record_id, formatted_text]):
                                    logger.warning(f"Record missing required fields: id={record_id}, has_text={bool(formatted_text)}")
                                    failed_count += 1
                                    continue
                                
                                # Check for duplicate record IDs
                                if record_id in processed_record_ids:
                                    logger.warning(f"DUPLICATE DETECTED: Record ID {record_id} already processed - skipping")
                                    failed_count += 1
                                    continue
                                
                                # Add to processed set
                                processed_record_ids.add(record_id)
                                
                                # Sanitize text
                                sanitized_text = sanitize_text(formatted_text)
                                
                                if not sanitized_text:
                                    logger.warning(f"Record {record_id} has empty text after sanitization")
                                    failed_count += 1
                                    continue
                                
                                # Generate embedding
                                embedding = openai_client.get_embedding(sanitized_text)
                                
                                # Use provided question_id for S3 CSV as well
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
                                    'data_source': data_source,
                                    'start_date': start_date,
                                    'end_date': end_date,
                                    'task_id': task_id,
                                    's3_key': s3_key
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
                                
                                # Update progress
                                dynamodb_client.increment_processed_records(task_id)
                                
                                # Log progress
                                if processed_count % 100 == 0:
                                    logger.warning(f"Processed {processed_count} records (chunk {chunk_count})")
                                
                                # Batch upsert every 500 vectors to manage memory
                                # Check if any namespace has accumulated enough vectors
                                should_upsert = False
                                for ns, vectors in vectors_by_namespace.items():
                                    if len(vectors) >= 500:
                                        should_upsert = True
                                        break
                                
                                if should_upsert:
                                    # Initialize PineconeClient only when needed
                                    if 'pinecone_client' not in locals():
                                        try:
                                            logger.warning("Initializing PineconeClient...")
                                            pinecone_client = PineconeClient()
                                            logger.warning("PineconeClient initialized successfully")
                                        except Exception as e:
                                            logger.error(f"CRITICAL: Error initializing PineconeClient: {e}")
                                            dynamodb_client.set_task_error(task_id, f"Pinecone initialization failed: {e}")
                                            raise
                                    
                                    # Upsert vectors for namespaces with >= 500 vectors
                                    for ns, vectors in list(vectors_by_namespace.items()):
                                        if len(vectors) >= 500:
                                            try:
                                                logger.warning(f"Upserting batch of {len(vectors)} vectors to namespace '{ns}'")
                                                pinecone_response = pinecone_client.upsert_vectors(vectors, namespace=ns)
                                                batch_upserted_count = pinecone_response.get('upserted_count', 0)
                                                total_upserted_count += batch_upserted_count
                                                logger.warning(f"Batch upserted: {batch_upserted_count} vectors to namespace '{ns}'. Total: {total_upserted_count}")
                                                
                                                # Clear the batch for this namespace
                                                vectors_by_namespace[ns] = []
                                                
                                            except Exception as upsert_error:
                                                logger.error(f"CRITICAL: Pinecone upsert failed for namespace '{ns}': {type(upsert_error).__name__}: {upsert_error}")
                                                dynamodb_client.set_task_error(task_id, f"Pinecone upsert failed: {upsert_error}")
                                                raise
                                
                            except Exception as e:
                                logger.error(f"Error processing record {record_id}: {e}")
                                failed_count += 1
                                dynamodb_client.increment_failed_records(task_id)
                                
                    # Final batch upload if any vectors remain
                    has_remaining = any(len(vectors) > 0 for vectors in vectors_by_namespace.values())
                    if has_remaining:
                        logger.warning(f"Upserting final batches to Pinecone")
                        
                        # Initialize PineconeClient if not already done
                        if 'pinecone_client' not in locals():
                            try:
                                logger.warning("Initializing PineconeClient...")
                                pinecone_client = PineconeClient()
                                logger.warning("PineconeClient initialized successfully")
                            except Exception as e:
                                logger.error(f"CRITICAL: Error initializing PineconeClient: {e}")
                                dynamodb_client.set_task_error(task_id, f"Pinecone initialization failed: {e}")
                                raise
                        
                        # Upsert remaining vectors for each namespace
                        for ns, vectors in vectors_by_namespace.items():
                            if vectors:
                                try:
                                    logger.warning(f"Upserting final batch of {len(vectors)} vectors to namespace '{ns}'")
                                    pinecone_response = pinecone_client.upsert_vectors(vectors, namespace=ns)
                                    batch_upserted_count = pinecone_response.get('upserted_count', 0)
                                    total_upserted_count += batch_upserted_count
                                    logger.warning(f"Final batch upserted: {batch_upserted_count} vectors to namespace '{ns}'. Total: {total_upserted_count}")
                                    
                                except Exception as upsert_error:
                                    logger.error(f"CRITICAL: Final Pinecone upsert failed for namespace '{ns}': {type(upsert_error).__name__}: {upsert_error}")
                                    dynamodb_client.set_task_error(task_id, f"Final Pinecone upsert failed: {upsert_error}")
                                    raise
                    
                    # Set totals for summary
                    total_records = processed_count + failed_count
                    source_data = []  # Empty since we processed in chunks
                    
                except Exception as e:
                    error_msg = f"Chunked CSV processing failed: {e}"
                    logger.error(error_msg)
                    dynamodb_client.set_task_error(task_id, error_msg)
                    raise
                
                # Skip the normal processing loop since we handled everything in chunks
                logger.warning(f"Chunked processing completed: {processed_count} processed, {failed_count} failed")
                
                # Update task status to completed
                dynamodb_client.update_task_status(
                    task_id, 
                    'COMPLETED',
                    total_records=total_records,
                    processed_records=processed_count,
                    failed_records=failed_count,
                    total_upserted_to_pinecone=total_upserted_count
                )
                
                logger.warning(f"Large CSV processing completed for task {task_id}")
                
                # Release the processing lock
                dynamodb_client.release_processing_lock(f"{start_date}{lock_key_suffix}", f"{end_date}{lock_key_suffix}", lambda_request_id)
                
                return  # Exit early since we handled everything
            
            # Update total records count
            total_records = len(source_data)
            
            # Check for duplicates in source data
            source_record_ids = [record.get('id') for record in source_data if record.get('id')]
            unique_source_ids = set(source_record_ids)
            
            logger.warning(f"=== SOURCE DATA ANALYSIS ===")
            logger.warning(f"Data source: {data_source}")
            logger.warning(f"Total records from source: {total_records}")
            logger.warning(f"Records with valid IDs: {len(source_record_ids)}")
            logger.warning(f"Unique record IDs: {len(unique_source_ids)}")
            if len(source_record_ids) != len(unique_source_ids):
                logger.warning(f"DUPLICATE IDs IN SOURCE DATA: {len(source_record_ids) - len(unique_source_ids)} duplicates detected!")
            logger.warning(f"Date range: {start_date} to {end_date}")
            logger.warning(f"=== START PROCESSING ===")
            
            dynamodb_client.update_task_status(task_id, 'IN_PROGRESS', total_records=total_records)
            
            # Process each record with OpenAI embeddings and Pinecone storage
            processed_count = 0
            failed_count = 0
            vectors_to_upsert = []
            processed_record_ids = set()  # Track unique record IDs to prevent duplicates
            total_upserted_count = 0  # Track total records upserted across all batches
            
            for i, record in enumerate(source_data):
                try:
                    # Extract required fields
                    record_id = record.get('id')
                    event_id = record.get('event_id')
                    org_id = record.get('org_id')
                    formatted_text = record.get('formatted_text')
                    
                    # Validate required fields
                    if not all([record_id, event_id, org_id, formatted_text]):
                        logger.warning(f"Record {i} missing required fields: {record}")
                        failed_count += 1
                        continue
                    
                    # Check for duplicate record IDs
                    if record_id in processed_record_ids:
                        logger.warning(f"DUPLICATE DETECTED: Record ID {record_id} already processed - skipping")
                        failed_count += 1
                        continue
                    
                    # Add to processed set
                    processed_record_ids.add(record_id)
                    logger.debug(f"Processing record {i}: ID={record_id}, Event={event_id}, Org={org_id}")
                    
                    # Sanitize text
                    sanitized_text = sanitize_text(formatted_text)
                    
                    if not sanitized_text:
                        logger.warning(f"Record {i} has empty text after sanitization")
                        failed_count += 1
                        continue
                    
                    # Generate embedding
                    embedding = openai_client.get_embedding(sanitized_text)
                    
                    # Prepare vector data for Pinecone
                    # Use question_id for vector ID in both cases
                    if data_source == 'metabase':
                        actual_question_id = metabase_client.question_id
                        vector_id = f"{actual_question_id}.{record_id}"
                    else:  # s3_csv
                        # Use provided question_id for S3 CSV as well
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
                        'data_source': data_source,
                        'start_date': start_date,
                        'end_date': end_date,
                        'task_id': task_id
                    })
                    
                    # Add source-specific metadata
                    if data_source == 's3_csv':
                        metadata['s3_key'] = s3_key
                        metadata['date_column'] = date_column
                    
                    # Convert all values to strings for Pinecone compatibility
                    metadata = {k: str(v) for k, v in metadata.items()}
                    
                    vector_data = {
                        'id': vector_id,
                        'values': embedding,
                        'metadata': metadata
                    }
                    
                    vectors_to_upsert.append(vector_data)
                    processed_count += 1
                    
                    # Update progress
                    dynamodb_client.increment_processed_records(task_id)
                    
                    # Log progress
                    if processed_count % 10 == 0:
                        logger.warning(f"Processed {processed_count}/{total_records} records")
                    
                except Exception as e:
                    logger.error(f"Error processing record {i}: {e}")
                    failed_count += 1
                    dynamodb_client.increment_failed_records(task_id)
            
            # Upsert vectors to Pinecone
            if vectors_to_upsert:
                logger.warning(f"Preparing to upsert {len(vectors_to_upsert)} vectors to Pinecone")
                logger.warning(f"Sample vector data: {vectors_to_upsert[0] if vectors_to_upsert else 'No vectors'}")
                
                # Initialize PineconeClient only when needed
                try:
                    logger.warning("Initializing PineconeClient...")
                    pinecone_client = PineconeClient()
                    logger.warning("PineconeClient initialized successfully")
                except Exception as e:
                    logger.error(f"CRITICAL: Error initializing PineconeClient: {e}")
                    dynamodb_client.set_task_error(task_id, f"Pinecone initialization failed: {e}")
                    raise
                
                try:
                    logger.warning(f"Calling pinecone_client.upsert_vectors() with {len(vectors_to_upsert)} vectors")
                    logger.warning(f"Vector IDs being upserted: {[v.get('id') for v in vectors_to_upsert[:5]]}{'...' if len(vectors_to_upsert) > 5 else ''}")
                    
                    pinecone_response = pinecone_client.upsert_vectors(vectors_to_upsert)
                    logger.warning(f"Pinecone upsert response received: {pinecone_response}")
                    
                    # Track total upserted count
                    batch_upserted_count = pinecone_response.get('upserted_count', 0)
                    total_upserted_count += batch_upserted_count
                    
                    # Verify the upsert was successful
                    if batch_upserted_count > 0:
                        logger.warning(f"SUCCESS: {batch_upserted_count} vectors upserted in this batch. TOTAL UPSERTED: {total_upserted_count}")
                    else:
                        logger.warning(f"WARNING: Pinecone reported 0 vectors upserted in this batch. Response: {pinecone_response}")
                        
                except Exception as upsert_error:
                    logger.error(f"CRITICAL: Pinecone upsert failed: {type(upsert_error).__name__}: {upsert_error}")
                    dynamodb_client.set_task_error(task_id, f"Pinecone upsert failed: {upsert_error}")
                    raise
            else:
                logger.warning("No vectors to upsert - vectors_to_upsert list is empty")
            
            # Log final summary
            unique_records_processed = len(processed_record_ids)
            logger.warning(f"=== PROCESSING SUMMARY ===")
            logger.warning(f"Data source: {data_source}")
            logger.warning(f"Source records received: {total_records}")
            logger.warning(f"Unique records processed: {unique_records_processed}")
            logger.warning(f"Successfully processed: {processed_count}")
            logger.warning(f"Failed/skipped records: {failed_count}")
            logger.warning(f"TOTAL VECTORS UPSERTED TO PINECONE: {total_upserted_count}")
            logger.warning(f"=== END SUMMARY ===")
            
            # Verify counts match
            if processed_count != total_upserted_count:
                logger.warning(f"MISMATCH: Processed {processed_count} records but upserted {total_upserted_count} vectors!")
            
            # Update task status to completed
            dynamodb_client.update_task_status(
                task_id, 
                'COMPLETED',
                processed_records=processed_count,
                failed_records=failed_count,
                total_upserted_to_pinecone=total_upserted_count
            )
            
            logger.warning(f"Async processing completed for task {task_id}")
            
            # Release the processing lock
            dynamodb_client.release_processing_lock(f"{start_date}{lock_key_suffix}", f"{end_date}{lock_key_suffix}", lambda_request_id)
            
        except Exception as e:
            logger.error(f"Error during async processing: {e}")
            dynamodb_client.set_task_error(task_id, str(e))
            # Release lock on error
            dynamodb_client.release_processing_lock(f"{start_date}{lock_key_suffix}", f"{end_date}{lock_key_suffix}", lambda_request_id)
            raise
            
    except Exception as e:
        logger.error(f"Async processing error: {e}")
        # Try to release lock if we have the variables available
        try:
            if 'dynamodb_client' in locals() and 'start_date' in locals() and 'end_date' in locals() and 'lambda_request_id' in locals():
                lock_key_suffix = f"_q{question_id}" if 'question_id' in locals() and question_id else ""
                dynamodb_client.release_processing_lock(f"{start_date}{lock_key_suffix}", f"{end_date}{lock_key_suffix}", lambda_request_id)
        except:
            pass  # Don't fail on cleanup
        return