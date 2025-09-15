"""
Enhanced chunk processor with streaming and multiprocessing capabilities.
Integrates CSV streaming, multiprocessing, and progress tracking for large-scale processing.
"""

import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import traceback
import uuid

from app.config import settings
from app.services.dynamodb_client import DynamoDBClient
from app.utils.csv_streaming import get_csv_streamer, analyze_csv_structure
from app.utils.multiprocessing_handler import (
    MultiprocessingCSVProcessor,
    process_csv_with_multiprocessing
)
from app.utils.progress_tracker import ProgressTracker

logger = logging.getLogger(__name__)


class EnhancedChunkProcessor:
    """
    Enhanced CSV chunk processor with streaming and multiprocessing.
    """
    
    def __init__(
        self,
        task_id: str,
        file_path: str,
        question_id: str = None,
        namespace: str = None,
        use_local: bool = None,
        max_workers: int = None,
        chunk_size: int = None,
        batch_size: int = None
    ):
        """
        Initialize enhanced chunk processor.
        
        Args:
            task_id: Unique task identifier
            file_path: Path to CSV file (S3 key or local path)
            question_id: Question ID for vector ID formatting (e.g., "10")
            namespace: Pinecone namespace
            use_local: Whether to use local filesystem
            max_workers: Maximum number of worker processes
            chunk_size: Size of chunks for processing
            batch_size: Batch size for Pinecone upserts
        """
        self.task_id = task_id
        self.file_path = file_path
        self.question_id = question_id
        self.namespace = namespace or settings.PINECONE_NAMESPACE
        self.use_local = use_local if use_local is not None else settings.USE_LOCAL_FILES
        self.max_workers = max_workers or settings.MAX_WORKERS
        self.chunk_size = chunk_size or settings.CHUNK_SIZE
        self.batch_size = batch_size or settings.PINECONE_BATCH_SIZE
        
        # Initialize components
        self.dynamodb = DynamoDBClient()
        self.csv_streamer = get_csv_streamer(self.use_local)
        self.multiprocessor = MultiprocessingCSVProcessor(
            max_workers=self.max_workers,
            openai_rate_limit=settings.OPENAI_RATE_LIMIT,
            pinecone_batch_size=self.batch_size
        )
        self.progress_tracker = ProgressTracker()
        if self.progress_tracker:
            self.progress_tracker.set_task_id(self.task_id)
        # self.progress_tracker = None  # Removed - using real tracker
        
        # Processing state
        self.start_time = None
        self.csv_analysis = None
        self.processing_results = None
    
    def analyze_file(self) -> Dict[str, Any]:
        """
        Analyze CSV file structure and prepare for processing.
        
        Returns:
            Analysis results including row count, columns, and size estimates
        """
        logger.info(f"Analyzing CSV file: {self.file_path}")
        
        try:
            self.csv_analysis = analyze_csv_structure(self.file_path, self.use_local)
            
            # Optimize chunk size based on file characteristics
            if self.csv_analysis['total_rows'] > 0:
                optimized_chunk_size = self.multiprocessor.optimize_chunk_size(
                    total_records=self.csv_analysis['total_rows'],
                    avg_record_size=self.csv_analysis.get('avg_row_size_bytes', 1024)
                )
                
                # Update chunk size if optimization suggests different value
                if optimized_chunk_size != self.chunk_size:
                    logger.info(f"Optimizing chunk size from {self.chunk_size} to {optimized_chunk_size}")
                    self.chunk_size = optimized_chunk_size
            
            # Calculate processing estimates
            total_chunks = (
                (self.csv_analysis['total_rows'] + self.chunk_size - 1) // self.chunk_size
                if self.csv_analysis['total_rows'] > 0
                else 0
            )
            
            estimated_memory_gb = self.multiprocessor.estimate_memory_usage(
                self.chunk_size,
                self.csv_analysis.get('avg_row_size_bytes', 1024)
            )
            
            # Add processing estimates to analysis
            self.csv_analysis.update({
                'chunk_size': self.chunk_size,
                'total_chunks': total_chunks,
                'max_workers': self.max_workers,
                'estimated_memory_gb': round(estimated_memory_gb, 2),
                'estimated_processing_time_minutes': self._estimate_processing_time(
                    self.csv_analysis['total_rows']
                )
            })
            
            logger.info(f"CSV analysis complete: {self.csv_analysis['total_rows']} rows, "
                       f"{total_chunks} chunks, {estimated_memory_gb:.2f} GB memory required")
            
            return self.csv_analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze CSV file: {e}")
            raise
    
    def _estimate_processing_time(self, total_rows: int) -> float:
        """
        Estimate processing time based on row count.
        
        Args:
            total_rows: Total number of rows to process
            
        Returns:
            Estimated time in minutes
        """
        # Rough estimates based on typical processing rates
        # Adjust based on your actual performance metrics
        rows_per_second = 50  # Conservative estimate with OpenAI rate limiting
        effective_parallelism = min(self.max_workers, 5)  # Account for rate limits
        
        estimated_seconds = total_rows / (rows_per_second * effective_parallelism)
        estimated_minutes = estimated_seconds / 60
        
        # Add overhead for initialization and finalization
        overhead_minutes = 1
        
        return round(estimated_minutes + overhead_minutes, 1)
    
    def process_streaming(self) -> Dict[str, Any]:
        """
        Process CSV file using streaming approach for memory efficiency.
        
        Returns:
            Processing results
        """
        logger.info(f"Starting streaming processing for task {self.task_id}")
        self.start_time = time.time()
        
        try:
            # Update task status
            if self.progress_tracker:
                self.progress_tracker.update_status(
                    'IN_PROGRESS',
                    message="Starting streaming CSV processing",
                    started_at=datetime.utcnow().isoformat()
                )
            else:
                # Fallback: Direct update when ProgressTracker is disabled
                logger.info(f"ProgressTracker disabled, updating task status directly for {self.task_id}")
                self.dynamodb.update_task_status(
                    self.task_id, 'processing',
                    started_at=datetime.utcnow().isoformat(),
                    message="Starting streaming CSV processing"
                )
                logger.info(f"Task {self.task_id} status updated to IN_PROGRESS")
            
            # Analyze file if not already done
            if not self.csv_analysis:
                self.analyze_file()
            
            if self.csv_analysis['total_rows'] == 0:
                logger.warning(f"CSV file is empty: {self.file_path}")
                return self._complete_empty_file()
            
            # Update task with total records count
            if not self.progress_tracker:
                self.dynamodb.update_task_status(
                    self.task_id, 'processing',
                    total_records=self.csv_analysis['total_rows']
                )
            
            # Initialize processing state
            total_processed = 0
            total_failed = 0
            total_vectors = 0
            chunk_results = []
            
            # Process file in streaming chunks
            chunk_index = 0
            for csv_chunk in self.csv_streamer.stream_chunks(self.file_path):
                chunk_id = f"chunk_{chunk_index:04d}"
                
                try:
                    # Update progress
                    if self.progress_tracker:
                        self.progress_tracker.update_chunk_status(
                            chunk_id=chunk_id,
                            status='IN_PROGRESS'
                        )
                    
                    # Process chunk with multiprocessing
                    chunk_result = self._process_single_chunk(
                        chunk_data=csv_chunk,
                        chunk_id=chunk_id,
                        chunk_index=chunk_index
                    )
                    
                    # Update counters
                    total_processed += chunk_result['processed']
                    total_failed += chunk_result['failed']
                    total_vectors += chunk_result['vectors_upserted']
                    chunk_results.append(chunk_result)
                    
                    # Update chunk status
                    if self.progress_tracker:
                        self.progress_tracker.update_chunk_status(
                            chunk_id=chunk_id,
                            status='COMPLETED',
                            processed_records=chunk_result['processed'],
                            failed_records=chunk_result['failed'],
                            vectors_upserted=chunk_result['vectors_upserted']
                        )
                    
                    # Report overall progress
                    progress_pct = ((chunk_index + 1) / self.csv_analysis['total_chunks']) * 100
                    if self.progress_tracker:
                        self.progress_tracker.report_progress(
                            current=chunk_index + 1,
                            total=self.csv_analysis['total_chunks'],
                            progress=progress_pct,
                            message=f"Processed chunk {chunk_index + 1}/{self.csv_analysis['total_chunks']}"
                        )
                    elif chunk_index % 5 == 0 or chunk_index == 0:  # Update every 5 chunks or first chunk
                        # Fallback: Direct progress update when ProgressTracker is disabled
                        # Only update metadata, not processed_records (let chunks handle that)
                        current_task = self.dynamodb.get_task(self.task_id)
                        if current_task:
                            # Only update if our count is higher (avoids regression)
                            current_processed = current_task.get('processed_records', 0)
                            if total_processed > current_processed:
                                self.dynamodb.update_task_progress(
                                    self.task_id,
                                    processed_records=total_processed,
                                    total_records=self.csv_analysis['total_rows'],
                                    status='processing',
                                    metadata={
                                        'current_chunk': chunk_index + 1,
                                        'total_chunks': self.csv_analysis['total_chunks'],
                                        'progress_percentage': progress_pct
                                    }
                                )
                            else:
                                # Just update metadata without touching processed_records
                                self.dynamodb.update_task_status(
                                    self.task_id,
                                    status='processing',
                                    current_chunk=chunk_index + 1,
                                    total_chunks=self.csv_analysis['total_chunks'],
                                    progress_percentage=progress_pct
                                )
                    
                except Exception as e:
                    logger.error(f"Failed to process chunk {chunk_id}: {e}")
                    if self.progress_tracker:
                        self.progress_tracker.update_chunk_status(
                            chunk_id=chunk_id,
                            status='FAILED',
                            error_message=str(e)
                        )
                    # Continue with next chunk
                
                chunk_index += 1
            
            # Calculate final results
            processing_time = time.time() - self.start_time
            
            self.processing_results = {
                'status': 'completed',
                'task_id': self.task_id,
                'file_path': self.file_path,
                'total_rows': self.csv_analysis['total_rows'],
                'total_chunks': chunk_index,
                'total_processed': total_processed,
                'total_failed': total_failed,
                'vectors_upserted': total_vectors,
                'processing_time_seconds': round(processing_time, 2),
                'processing_rate': round(total_processed / processing_time, 2) if processing_time > 0 else 0,
                'chunk_results': chunk_results
            }
            
            # Update final task status
            if self.progress_tracker:
                self.progress_tracker.update_status(
                    'COMPLETED',
                    processed_records=total_processed,
                    failed_records=total_failed,
                    vectors_upserted=total_vectors,
                    completed_at=datetime.utcnow().isoformat(),
                    processing_time_seconds=processing_time
                )
            else:
                # Fallback: Direct update when ProgressTracker is disabled
                self.dynamodb.update_task_status(
                    self.task_id, 'completed',
                    processed_records=total_processed,
                    failed_records=total_failed,
                    vectors_upserted=total_vectors,
                    completed_at=datetime.utcnow().isoformat(),
                    processing_time_seconds=round(processing_time, 2)
                )
            
            logger.info(f"Completed streaming processing: {total_processed} processed, "
                       f"{total_failed} failed, {total_vectors} vectors upserted in {processing_time:.2f}s")
            
            return self.processing_results
            
        except Exception as e:
            logger.error(f"Streaming processing failed: {e}")
            logger.error(traceback.format_exc())
            
            if self.progress_tracker:
                self.progress_tracker.set_error(
                    error_message=str(e),
                    traceback=traceback.format_exc()
                )
            
            raise
    
    def process_parallel_chunks(self) -> Dict[str, Any]:
        """
        Process CSV file with parallel chunk processing.
        Best for files that fit in memory.
        
        Returns:
            Processing results
        """
        logger.info(f"Starting parallel chunk processing for task {self.task_id}")
        self.start_time = time.time()
        
        try:
            # Update task status
            if self.progress_tracker:
                self.progress_tracker.update_status(
                    'IN_PROGRESS',
                    message="Loading CSV for parallel processing",
                    started_at=datetime.utcnow().isoformat()
                )
            
            # Analyze file if not already done
            if not self.csv_analysis:
                self.analyze_file()
            
            if self.csv_analysis['total_rows'] == 0:
                logger.warning(f"CSV file is empty: {self.file_path}")
                return self._complete_empty_file()
            
            # Check if file size is suitable for parallel processing
            if self.csv_analysis['estimated_size_mb'] > 1000:  # 1GB threshold
                logger.warning(f"File too large ({self.csv_analysis['estimated_size_mb']} MB) for parallel processing, "
                              "switching to streaming mode")
                return self.process_streaming()
            
            # Load all data into memory
            logger.info("Loading CSV data into memory for parallel processing")
            all_data = []
            for chunk in self.csv_streamer.stream_chunks(self.file_path):
                all_data.extend(chunk)
            
            logger.info(f"Loaded {len(all_data)} rows, starting parallel processing")
            
            # Process with multiprocessing
            def progress_callback(progress_info):
                if self.progress_tracker:
                    self.progress_tracker.report_progress(
                        current=progress_info['completed_chunks'],
                        total=progress_info['total_chunks'],
                        progress=progress_info['progress'],
                        message=f"Processing chunk {progress_info.get('current_chunk', '')}"
                    )
            
            results = process_csv_with_multiprocessing(
                csv_data=all_data,
                task_id=self.task_id,
                namespace=self.namespace,
                chunk_size=self.chunk_size,
                max_workers=self.max_workers,
                progress_callback=progress_callback
            )
            
            # Calculate final results
            processing_time = time.time() - self.start_time
            
            self.processing_results = {
                'status': 'completed',
                'task_id': self.task_id,
                'file_path': self.file_path,
                'total_rows': len(all_data),
                'total_chunks': len(results.get('chunk_results', [])),
                'total_processed': results['total_processed'],
                'total_failed': results['total_failed'],
                'vectors_upserted': results['total_vectors'],
                'processing_time_seconds': round(processing_time, 2),
                'processing_rate': round(results['total_processed'] / processing_time, 2) if processing_time > 0 else 0,
                'chunk_results': results.get('chunk_results', []),
                'errors': results.get('errors', [])
            }
            
            # Update final task status
            if self.progress_tracker:
                self.progress_tracker.update_status(
                    'COMPLETED',
                    processed_records=results['total_processed'],
                    failed_records=results['total_failed'],
                    vectors_upserted=results['total_vectors'],
                    completed_at=datetime.utcnow().isoformat(),
                    processing_time_seconds=processing_time
                )
            
            logger.info(f"Completed parallel processing: {results['total_processed']} processed, "
                       f"{results['total_failed']} failed, {results['total_vectors']} vectors upserted "
                       f"in {processing_time:.2f}s")
            
            return self.processing_results
            
        except Exception as e:
            logger.error(f"Parallel processing failed: {e}")
            logger.error(traceback.format_exc())
            
            if self.progress_tracker:
                self.progress_tracker.set_error(
                    error_message=str(e),
                    traceback=traceback.format_exc()
                )
            
            raise
    
    def _process_single_chunk(
        self,
        chunk_data: List[Dict[str, Any]],
        chunk_id: str,
        chunk_index: int
    ) -> Dict[str, Any]:
        """
        Process a single chunk of data.
        
        Args:
            chunk_data: List of records in the chunk
            chunk_id: Unique chunk identifier
            chunk_index: Index of the chunk
            
        Returns:
            Chunk processing results
        """
        from app.services.openai_client import OpenAIClient
        from app.services.pinecone_client import PineconeClient
        from app.utils.text_sanitizer import sanitize_text
        
        openai_client = OpenAIClient()
        pinecone_client = PineconeClient()
        
        result = {
            'chunk_id': chunk_id,
            'chunk_index': chunk_index,
            'total_records': len(chunk_data),
            'processed': 0,
            'failed': 0,
            'vectors_upserted': 0
        }
        
        # Group vectors by namespace
        vectors_by_namespace = {}
        
        for i, record in enumerate(chunk_data):
            try:
                # Get text content based on record type
                if isinstance(record, dict):
                    # For CSV records, use formatted_text field
                    text_content = record.get('formatted_text', '')
                    if not text_content:
                        # Fallback to sanitizing the entire record as text
                        text_content = sanitize_text(str(record))
                    else:
                        text_content = sanitize_text(text_content)
                    
                    # Get row ID for vector ID
                    row_id = record.get('id', f"unknown_{i}")
                    
                    # Get namespace from record_type
                    record_namespace = record.get('record_type', self.namespace)
                else:
                    # For non-dict records, sanitize as string
                    text_content = sanitize_text(str(record))
                    row_id = f"unknown_{i}"
                    record_namespace = self.namespace
                
                if not text_content.strip():
                    result['failed'] += 1
                    continue
                
                # Generate embedding
                try:
                    embedding = openai_client.get_embedding(text_content)
                except Exception as e:
                    logger.warning(f"Failed to generate embedding for chunk {chunk_id}, record {i}: {e}")
                    result['failed'] += 1
                    continue
                
                # Create vector ID in format: question_id.row_id
                if self.question_id:
                    vector_id = f"{self.question_id}.{row_id}"
                else:
                    # Fallback if no question_id provided
                    vector_id = f"{self.task_id}_{chunk_id}_{i}"
                
                # Prepare metadata
                metadata = {
                    'text': text_content[:1000],
                    'chunk_id': chunk_id,
                    'record_index': i,
                    'task_id': self.task_id,
                    'question_id': self.question_id or '',
                    'row_id': row_id,
                    'file_path': self.file_path,
                    'processed_at': datetime.utcnow().isoformat()
                }
                
                # Add all additional metadata from record if it's a dict
                if isinstance(record, dict):
                    # Add ALL fields from CSV (except formatted_text which becomes 'text')
                    for field, value in record.items():
                        # Skip formatted_text as it's already stored as 'text'
                        # Skip fields that are already in metadata
                        if field != 'formatted_text' and field not in metadata and value:
                            # Convert value to string to ensure compatibility
                            metadata[field] = str(value)
                
                vector_data = {
                    'id': vector_id,
                    'values': embedding,
                    'metadata': metadata
                }
                
                # Group by namespace
                namespace_to_use = record_namespace or self.namespace
                if namespace_to_use not in vectors_by_namespace:
                    vectors_by_namespace[namespace_to_use] = []
                vectors_by_namespace[namespace_to_use].append(vector_data)
                
                # Batch upsert when any namespace reaches batch size
                for ns, vectors in vectors_by_namespace.items():
                    if len(vectors) >= self.batch_size:
                        try:
                            pinecone_client.upsert_vectors(vectors, namespace=ns)
                            result['vectors_upserted'] += len(vectors)
                            result['processed'] += len(vectors)
                            vectors_by_namespace[ns] = []
                        except Exception as e:
                            logger.error(f"Failed to upsert batch to namespace {ns} in chunk {chunk_id}: {e}")
                            result['failed'] += len(vectors)
                            vectors_by_namespace[ns] = []
                
            except Exception as e:
                logger.error(f"Error processing record {i} in chunk {chunk_id}: {e}")
                result['failed'] += 1
        
        # Upsert remaining vectors for all namespaces
        for ns, vectors in vectors_by_namespace.items():
            if vectors:
                try:
                    pinecone_client.upsert_vectors(vectors, namespace=ns)
                    result['vectors_upserted'] += len(vectors)
                    result['processed'] += len(vectors)
                except Exception as e:
                    logger.error(f"Failed to upsert final batch to namespace {ns} in chunk {chunk_id}: {e}")
                    result['failed'] += len(vectors)
        
        return result
    
    def _complete_empty_file(self) -> Dict[str, Any]:
        """
        Handle completion for empty CSV file.
        
        Returns:
            Results for empty file
        """
        self.processing_results = {
            'status': 'completed',
            'task_id': self.task_id,
            'file_path': self.file_path,
            'total_rows': 0,
            'total_chunks': 0,
            'total_processed': 0,
            'total_failed': 0,
            'vectors_upserted': 0,
            'message': 'CSV file is empty'
        }
        
        if self.progress_tracker:
            self.progress_tracker.update_status(
                'COMPLETED',
                processed_records=0,
                failed_records=0,
                completed_at=datetime.utcnow().isoformat(),
                message="CSV file is empty"
            )
        
        return self.processing_results
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current processing status.
        
        Returns:
            Current status information
        """
        status = self.dynamodb.get_task(self.task_id)
        
        if status and self.processing_results:
            status['processing_results'] = self.processing_results
        
        if status and self.csv_analysis:
            status['file_analysis'] = self.csv_analysis
        
        return status


def process_csv_file(
    task_id: str,
    file_path: str,
    question_id: str = None,
    namespace: str = None,
    use_local: bool = None,
    streaming: bool = True,
    **kwargs
) -> Dict[str, Any]:
    """
    High-level function to process a CSV file.
    
    Args:
        task_id: Unique task identifier
        file_path: Path to CSV file
        question_id: Question ID for vector ID formatting (e.g., "10")
        namespace: Pinecone namespace
        use_local: Whether to use local filesystem
        streaming: Whether to use streaming (True) or parallel processing
        **kwargs: Additional parameters for processor
        
    Returns:
        Processing results
    """
    processor = EnhancedChunkProcessor(
        task_id=task_id,
        file_path=file_path,
        question_id=question_id,
        namespace=namespace,
        use_local=use_local,
        **kwargs
    )
    
    # Analyze file first
    analysis = processor.analyze_file()
    
    # Choose processing method based on file size and settings
    if streaming or analysis['estimated_size_mb'] > 1000:
        return processor.process_streaming()
    else:
        return processor.process_parallel_chunks()