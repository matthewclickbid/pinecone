"""
Multiprocessing handler for efficient CSV chunk processing.
Implements parallel processing with memory management and rate limiting.
"""

import logging
import multiprocessing as mp
from multiprocessing import Pool, Queue, Manager
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import traceback
from functools import partial
import signal
import psutil
import os

from app.config import settings
from app.services.openai_client import OpenAIClient
from app.services.pinecone_client import PineconeClient
from app.utils.text_sanitizer import sanitize_text

logger = logging.getLogger(__name__)


class RateLimiter:
    """Token bucket rate limiter for API calls."""
    
    def __init__(self, rate_limit: int = 20, interval: float = 1.0):
        """
        Initialize rate limiter.
        
        Args:
            rate_limit: Maximum requests per interval
            interval: Time interval in seconds
        """
        self.rate_limit = rate_limit
        self.interval = interval
        self.manager = Manager()
        self.tokens = self.manager.Value('i', rate_limit)
        self.last_refill = self.manager.Value('d', time.time())
        self.lock = self.manager.Lock()
    
    def acquire(self, timeout: float = 30.0) -> bool:
        """
        Acquire a token from the bucket.
        
        Args:
            timeout: Maximum time to wait for a token
            
        Returns:
            True if token acquired, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            with self.lock:
                # Refill tokens if needed
                current_time = time.time()
                elapsed = current_time - self.last_refill.value
                
                if elapsed >= self.interval:
                    tokens_to_add = int(elapsed / self.interval) * self.rate_limit
                    self.tokens.value = min(self.rate_limit, self.tokens.value + tokens_to_add)
                    self.last_refill.value = current_time
                
                # Try to acquire token
                if self.tokens.value > 0:
                    self.tokens.value -= 1
                    return True
            
            # Wait a bit before retrying
            time.sleep(0.05)
        
        return False


class MultiprocessingCSVProcessor:
    """Handles multiprocessing for CSV chunk processing."""
    
    def __init__(
        self,
        max_workers: Optional[int] = None,
        openai_rate_limit: int = 20,
        pinecone_batch_size: int = 100,
        memory_limit_gb: float = 0.8  # Use 80% of available memory
    ):
        """
        Initialize multiprocessing handler.
        
        Args:
            max_workers: Maximum number of worker processes
            openai_rate_limit: OpenAI API rate limit per second
            pinecone_batch_size: Batch size for Pinecone upserts
            memory_limit_gb: Memory usage limit as fraction of available
        """
        self.max_workers = max_workers or min(mp.cpu_count(), settings.MAX_WORKERS)
        self.openai_rate_limit = openai_rate_limit
        self.pinecone_batch_size = pinecone_batch_size
        self.memory_limit_gb = memory_limit_gb
        
        # Calculate memory limit
        total_memory = psutil.virtual_memory().total / (1024 ** 3)  # Convert to GB
        self.max_memory_gb = total_memory * memory_limit_gb
        
        logger.info(f"Initialized MultiprocessingCSVProcessor with {self.max_workers} workers, "
                   f"memory limit: {self.max_memory_gb:.2f} GB")
    
    def process_chunks_parallel(
        self,
        chunks: List[List[Dict[str, Any]]],
        task_id: str,
        namespace: str,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        Process multiple CSV chunks in parallel.
        
        Args:
            chunks: List of chunk data (each chunk is a list of records)
            task_id: Task ID for tracking
            namespace: Pinecone namespace
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dict with processing results
        """
        total_chunks = len(chunks)
        logger.info(f"Starting parallel processing of {total_chunks} chunks with {self.max_workers} workers")
        
        # Create rate limiter for OpenAI API
        manager = Manager()
        rate_limiter = RateLimiter(self.openai_rate_limit)
        
        # Create shared progress tracking
        progress_queue = manager.Queue()
        error_queue = manager.Queue()
        
        # Process chunks with ProcessPoolExecutor
        results = {
            'total_processed': 0,
            'total_failed': 0,
            'total_vectors': 0,
            'chunk_results': [],
            'errors': []
        }
        
        try:
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all chunk processing jobs
                future_to_chunk = {}
                
                for i, chunk_data in enumerate(chunks):
                    chunk_id = f"chunk_{i:04d}"
                    
                    # Check memory usage before submitting
                    current_memory = psutil.virtual_memory().percent
                    if current_memory > 90:
                        logger.warning(f"High memory usage ({current_memory}%), waiting before submitting chunk {chunk_id}")
                        time.sleep(5)
                    
                    future = executor.submit(
                        self._process_single_chunk,
                        chunk_data,
                        chunk_id,
                        task_id,
                        namespace,
                        rate_limiter,
                        progress_queue,
                        error_queue
                    )
                    future_to_chunk[future] = chunk_id
                
                # Process results as they complete
                completed = 0
                for future in as_completed(future_to_chunk):
                    chunk_id = future_to_chunk[future]
                    
                    try:
                        result = future.result(timeout=300)  # 5 minute timeout per chunk
                        results['total_processed'] += result['processed']
                        results['total_failed'] += result['failed']
                        results['total_vectors'] += result['vectors_upserted']
                        results['chunk_results'].append(result)
                        
                        completed += 1
                        
                        # Report progress
                        if progress_callback:
                            progress_callback({
                                'completed_chunks': completed,
                                'total_chunks': total_chunks,
                                'progress': (completed / total_chunks) * 100,
                                'current_chunk': chunk_id
                            })
                        
                        logger.debug(f"Completed chunk {chunk_id}: {result}")
                        
                    except Exception as e:
                        logger.error(f"Chunk {chunk_id} processing failed: {e}")
                        results['errors'].append({
                            'chunk_id': chunk_id,
                            'error': str(e),
                            'traceback': traceback.format_exc()
                        })
                        results['total_failed'] += len(chunk_data)
                
                # Collect any remaining errors from error queue
                while not error_queue.empty():
                    try:
                        error = error_queue.get_nowait()
                        results['errors'].append(error)
                    except:
                        break
        
        except Exception as e:
            logger.error(f"Parallel processing failed: {e}")
            raise
        
        logger.info(f"Completed parallel processing: {results['total_processed']} processed, "
                   f"{results['total_failed']} failed, {results['total_vectors']} vectors upserted")
        
        return results
    
    @staticmethod
    def _process_single_chunk(
        chunk_data: List[Dict[str, Any]],
        chunk_id: str,
        task_id: str,
        namespace: str,
        rate_limiter: RateLimiter,
        progress_queue: Queue,
        error_queue: Queue
    ) -> Dict[str, Any]:
        """
        Process a single chunk in a worker process.
        
        Args:
            chunk_data: List of records in the chunk
            chunk_id: Unique chunk identifier
            task_id: Task ID for tracking
            namespace: Pinecone namespace
            rate_limiter: Shared rate limiter
            progress_queue: Queue for progress updates
            error_queue: Queue for error reporting
            
        Returns:
            Dict with chunk processing results
        """
        # Set up signal handling for graceful shutdown
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        
        # Initialize clients (each process needs its own)
        openai_client = OpenAIClient()
        pinecone_client = PineconeClient()
        
        result = {
            'chunk_id': chunk_id,
            'total_records': len(chunk_data),
            'processed': 0,
            'failed': 0,
            'vectors_upserted': 0
        }
        
        vectors_to_upsert = []
        
        try:
            for i, record in enumerate(chunk_data):
                try:
                    # Sanitize text
                    text_content = sanitize_text(record)
                    if not text_content.strip():
                        result['failed'] += 1
                        continue
                    
                    # Rate limit for OpenAI API
                    if not rate_limiter.acquire(timeout=30):
                        logger.warning(f"Rate limit timeout for chunk {chunk_id}, record {i}")
                        result['failed'] += 1
                        continue
                    
                    # Generate embedding
                    try:
                        embedding = openai_client.get_embedding(text_content)
                    except Exception as e:
                        logger.warning(f"Failed to generate embedding for chunk {chunk_id}, record {i}: {e}")
                        result['failed'] += 1
                        continue
                    
                    # Prepare vector
                    vector_id = f"{task_id}_{chunk_id}_{i}"
                    vector_data = {
                        'id': vector_id,
                        'values': embedding,
                        'metadata': {
                            'text': text_content[:1000],
                            'chunk_id': chunk_id,
                            'record_index': i,
                            'task_id': task_id,
                            'source': 'csv_multiprocessing'
                        }
                    }
                    vectors_to_upsert.append(vector_data)
                    
                    # Batch upsert to Pinecone
                    if len(vectors_to_upsert) >= settings.PINECONE_BATCH_SIZE:
                        try:
                            pinecone_client.upsert_vectors(vectors_to_upsert, namespace=namespace)
                            result['vectors_upserted'] += len(vectors_to_upsert)
                            result['processed'] += len(vectors_to_upsert)
                            vectors_to_upsert = []
                        except Exception as e:
                            logger.error(f"Failed to upsert batch in chunk {chunk_id}: {e}")
                            result['failed'] += len(vectors_to_upsert)
                            vectors_to_upsert = []
                    
                    # Report progress periodically
                    if i % 100 == 0 and i > 0:
                        progress_queue.put({
                            'chunk_id': chunk_id,
                            'current': i,
                            'total': len(chunk_data),
                            'progress': (i / len(chunk_data)) * 100
                        })
                
                except Exception as e:
                    logger.error(f"Error processing record {i} in chunk {chunk_id}: {e}")
                    result['failed'] += 1
                    error_queue.put({
                        'chunk_id': chunk_id,
                        'record_index': i,
                        'error': str(e)
                    })
            
            # Upsert remaining vectors
            if vectors_to_upsert:
                try:
                    pinecone_client.upsert_vectors(vectors_to_upsert, namespace=namespace)
                    result['vectors_upserted'] += len(vectors_to_upsert)
                    result['processed'] += len(vectors_to_upsert)
                except Exception as e:
                    logger.error(f"Failed to upsert final batch in chunk {chunk_id}: {e}")
                    result['failed'] += len(vectors_to_upsert)
            
        except Exception as e:
            logger.error(f"Chunk {chunk_id} processing failed: {e}")
            error_queue.put({
                'chunk_id': chunk_id,
                'error': str(e),
                'traceback': traceback.format_exc()
            })
            result['failed'] = len(chunk_data)
        
        return result
    
    def estimate_memory_usage(self, chunk_size: int, record_size_bytes: int = 1024) -> float:
        """
        Estimate memory usage for processing.
        
        Args:
            chunk_size: Number of records per chunk
            record_size_bytes: Average size of each record
            
        Returns:
            Estimated memory usage in GB
        """
        # Estimate: record data + embedding (1536 floats * 4 bytes) + overhead
        embedding_size = 1536 * 4  # OpenAI embeddings
        overhead_factor = 2.0  # Account for Python overhead
        
        memory_per_record = (record_size_bytes + embedding_size) * overhead_factor
        memory_per_chunk = memory_per_record * chunk_size
        total_memory = memory_per_chunk * self.max_workers
        
        return total_memory / (1024 ** 3)  # Convert to GB
    
    def optimize_chunk_size(self, total_records: int, avg_record_size: int = 1024) -> int:
        """
        Calculate optimal chunk size based on available resources.
        
        Args:
            total_records: Total number of records to process
            avg_record_size: Average size of each record in bytes
            
        Returns:
            Optimal chunk size
        """
        # Start with configured chunk size
        base_chunk_size = settings.CHUNK_SIZE
        
        # Adjust based on memory constraints
        estimated_memory = self.estimate_memory_usage(base_chunk_size, avg_record_size)
        
        if estimated_memory > self.max_memory_gb:
            # Reduce chunk size to fit memory
            memory_ratio = self.max_memory_gb / estimated_memory
            optimized_chunk_size = int(base_chunk_size * memory_ratio * 0.8)  # 80% safety margin
        else:
            optimized_chunk_size = base_chunk_size
        
        # Ensure reasonable bounds
        min_chunk_size = 100
        max_chunk_size = 10000
        optimized_chunk_size = max(min_chunk_size, min(optimized_chunk_size, max_chunk_size))
        
        # Adjust for total records
        if total_records < optimized_chunk_size * self.max_workers:
            # If we have fewer records than optimal processing capacity
            optimized_chunk_size = max(min_chunk_size, total_records // self.max_workers)
        
        logger.info(f"Optimized chunk size: {optimized_chunk_size} "
                   f"(base: {base_chunk_size}, estimated memory: {estimated_memory:.2f} GB)")
        
        return optimized_chunk_size


def process_csv_with_multiprocessing(
    csv_data: List[Dict[str, Any]],
    task_id: str,
    namespace: str,
    chunk_size: Optional[int] = None,
    max_workers: Optional[int] = None,
    progress_callback: Optional[callable] = None
) -> Dict[str, Any]:
    """
    High-level function to process CSV data using multiprocessing.
    
    Args:
        csv_data: Complete CSV data as list of records
        task_id: Task ID for tracking
        namespace: Pinecone namespace
        chunk_size: Size of chunks (optional, will be optimized if not provided)
        max_workers: Maximum worker processes (optional)
        progress_callback: Callback for progress updates
        
    Returns:
        Dict with processing results
    """
    processor = MultiprocessingCSVProcessor(max_workers=max_workers)
    
    # Optimize chunk size if not provided
    if chunk_size is None:
        chunk_size = processor.optimize_chunk_size(len(csv_data))
    
    # Split data into chunks
    chunks = []
    for i in range(0, len(csv_data), chunk_size):
        chunk = csv_data[i:i + chunk_size]
        chunks.append(chunk)
    
    logger.info(f"Processing {len(csv_data)} records in {len(chunks)} chunks of size {chunk_size}")
    
    # Process chunks in parallel
    results = processor.process_chunks_parallel(
        chunks=chunks,
        task_id=task_id,
        namespace=namespace,
        progress_callback=progress_callback
    )
    
    return results