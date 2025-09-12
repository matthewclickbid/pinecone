"""
CSV streaming utilities for memory-efficient processing of large files.
Supports both S3 and local file streaming with chunk-based reading.
"""

import csv
import io
import logging
from typing import Iterator, Dict, Any, Optional, List, Tuple
import boto3
from botocore.exceptions import ClientError
import os
import tempfile
import gzip
import zipfile
from pathlib import Path

from app.config import settings

logger = logging.getLogger(__name__)


class CSVStreamer:
    """Base class for CSV streaming operations."""
    
    def __init__(self, chunk_size: int = 1000):
        """
        Initialize CSV streamer.
        
        Args:
            chunk_size: Number of rows to read in each chunk
        """
        self.chunk_size = chunk_size
    
    def count_rows(self, file_path: str) -> int:
        """
        Count total rows in CSV file.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Total number of rows (excluding header)
        """
        raise NotImplementedError
    
    def stream_chunks(self, file_path: str, skip_header: bool = True) -> Iterator[List[Dict[str, Any]]]:
        """
        Stream CSV file in chunks.
        
        Args:
            file_path: Path to CSV file
            skip_header: Whether to skip the header row
            
        Yields:
            Chunks of CSV data as list of dictionaries
        """
        raise NotImplementedError
    
    def get_sample(self, file_path: str, sample_size: int = 10) -> List[Dict[str, Any]]:
        """
        Get a sample of rows from the CSV.
        
        Args:
            file_path: Path to CSV file
            sample_size: Number of rows to sample
            
        Returns:
            Sample rows as list of dictionaries
        """
        raise NotImplementedError


class S3CSVStreamer(CSVStreamer):
    """Stream CSV files from S3 with memory efficiency."""
    
    def __init__(self, bucket_name: str, chunk_size: int = 1000):
        """
        Initialize S3 CSV streamer.
        
        Args:
            bucket_name: S3 bucket name
            chunk_size: Number of rows per chunk
        """
        super().__init__(chunk_size)
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
    
    def count_rows(self, s3_key: str) -> int:
        """
        Count rows in S3 CSV file.
        
        Args:
            s3_key: S3 object key
            
        Returns:
            Total number of rows (excluding header)
        """
        try:
            logger.info(f"Counting rows in s3://{self.bucket_name}/{s3_key}")
            
            # Get object size first to estimate
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            file_size = response['ContentLength']
            
            # For very large files, use sampling to estimate
            if file_size > 100 * 1024 * 1024:  # 100MB
                return self._estimate_rows(s3_key, file_size)
            
            # For smaller files, count exactly
            row_count = 0
            for chunk in self._stream_s3_lines(s3_key):
                lines = chunk.count('\n')
                row_count += lines
            
            # Subtract 1 for header
            return max(0, row_count - 1)
            
        except ClientError as e:
            logger.error(f"Error counting rows in S3 file: {e}")
            raise
    
    def _estimate_rows(self, s3_key: str, file_size: int) -> int:
        """
        Estimate row count for very large files.
        
        Args:
            s3_key: S3 object key
            file_size: Total file size in bytes
            
        Returns:
            Estimated row count
        """
        # Read first 1MB to estimate average row size
        sample_size = min(1024 * 1024, file_size)
        
        response = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Range=f'bytes=0-{sample_size-1}'
        )
        
        sample_data = response['Body'].read().decode('utf-8', errors='ignore')
        sample_lines = sample_data.count('\n')
        
        if sample_lines > 0:
            avg_line_size = sample_size / sample_lines
            estimated_rows = int(file_size / avg_line_size)
            logger.info(f"Estimated {estimated_rows} rows based on sample")
            return estimated_rows - 1  # Subtract header
        
        return 0
    
    def _stream_s3_lines(self, s3_key: str, chunk_bytes: int = 1024 * 1024) -> Iterator[str]:
        """
        Stream S3 file content in byte chunks.
        
        Args:
            s3_key: S3 object key
            chunk_bytes: Size of byte chunks to read
            
        Yields:
            String chunks of file content
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            
            # Handle compressed files
            if s3_key.endswith('.gz'):
                body = gzip.GzipFile(fileobj=response['Body'])
            elif s3_key.endswith('.zip'):
                # For zip files, extract first CSV
                with tempfile.TemporaryFile() as temp_file:
                    temp_file.write(response['Body'].read())
                    temp_file.seek(0)
                    with zipfile.ZipFile(temp_file) as zf:
                        csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                        if csv_files:
                            with zf.open(csv_files[0]) as csv_file:
                                while True:
                                    chunk = csv_file.read(chunk_bytes)
                                    if not chunk:
                                        break
                                    yield chunk.decode('utf-8', errors='ignore')
                return
            else:
                body = response['Body']
            
            # Stream content
            while True:
                chunk = body.read(chunk_bytes)
                if not chunk:
                    break
                yield chunk.decode('utf-8', errors='ignore')
                
        except ClientError as e:
            logger.error(f"Error streaming S3 file: {e}")
            raise
    
    def stream_chunks(self, s3_key: str, skip_header: bool = True) -> Iterator[List[Dict[str, Any]]]:
        """
        Stream S3 CSV file in chunks.
        
        Args:
            s3_key: S3 object key
            skip_header: Whether to skip header row
            
        Yields:
            Chunks of CSV data as list of dictionaries
        """
        logger.info(f"Starting to stream s3://{self.bucket_name}/{s3_key} in chunks of {self.chunk_size}")
        
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            
            # Handle compressed files
            if s3_key.endswith('.gz'):
                body = gzip.GzipFile(fileobj=response['Body'])
                lines = io.TextIOWrapper(body, encoding='utf-8', errors='ignore')
            elif s3_key.endswith('.zip'):
                # Handle zip files
                with tempfile.TemporaryFile() as temp_file:
                    temp_file.write(response['Body'].read())
                    temp_file.seek(0)
                    with zipfile.ZipFile(temp_file) as zf:
                        csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                        if csv_files:
                            lines = io.TextIOWrapper(
                                zf.open(csv_files[0]),
                                encoding='utf-8',
                                errors='ignore'
                            )
                        else:
                            logger.error("No CSV files found in zip archive")
                            return
            else:
                lines = io.TextIOWrapper(response['Body'], encoding='utf-8', errors='ignore')
            
            # Read CSV with DictReader
            csv_reader = csv.DictReader(lines)
            
            chunk = []
            for row in csv_reader:
                chunk.append(row)
                
                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = []
            
            # Yield remaining rows
            if chunk:
                yield chunk
                
        except ClientError as e:
            logger.error(f"Error streaming CSV from S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error streaming CSV: {e}")
            raise
    
    def get_sample(self, s3_key: str, sample_size: int = 10) -> List[Dict[str, Any]]:
        """
        Get sample rows from S3 CSV.
        
        Args:
            s3_key: S3 object key
            sample_size: Number of rows to sample
            
        Returns:
            Sample rows as list of dictionaries
        """
        sample = []
        
        for chunk in self.stream_chunks(s3_key):
            for row in chunk:
                sample.append(row)
                if len(sample) >= sample_size:
                    return sample
        
        return sample
    
    def stream_chunk_range(
        self,
        s3_key: str,
        start_row: int,
        end_row: int
    ) -> List[Dict[str, Any]]:
        """
        Stream a specific range of rows from S3 CSV.
        
        Args:
            s3_key: S3 object key
            start_row: Starting row index (0-based, excluding header)
            end_row: Ending row index (exclusive)
            
        Returns:
            List of rows in the specified range
        """
        logger.debug(f"Streaming rows {start_row} to {end_row} from s3://{self.bucket_name}/{s3_key}")
        
        result = []
        current_row = 0
        
        for chunk in self.stream_chunks(s3_key):
            for row in chunk:
                if current_row >= end_row:
                    return result
                
                if current_row >= start_row:
                    result.append(row)
                
                current_row += 1
        
        return result


class LocalCSVStreamer(CSVStreamer):
    """Stream CSV files from local filesystem."""
    
    def __init__(self, base_path: str, chunk_size: int = 1000):
        """
        Initialize local CSV streamer.
        
        Args:
            base_path: Base directory for CSV files
            chunk_size: Number of rows per chunk
        """
        super().__init__(chunk_size)
        self.base_path = Path(base_path)
    
    def count_rows(self, file_name: str) -> int:
        """
        Count rows in local CSV file.
        
        Args:
            file_name: Name of CSV file
            
        Returns:
            Total number of rows (excluding header)
        """
        file_path = self.base_path / file_name
        
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        # Use CSV reader to properly handle multi-line fields
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            csv_reader = csv.reader(f)
            row_count = sum(1 for _ in csv_reader) - 1  # Subtract header
        
        return max(0, row_count)
    
    def stream_chunks(self, file_name: str, skip_header: bool = True) -> Iterator[List[Dict[str, Any]]]:
        """
        Stream local CSV file in chunks.
        
        Args:
            file_name: Name of CSV file
            skip_header: Whether to skip header row
            
        Yields:
            Chunks of CSV data as list of dictionaries
        """
        file_path = self.base_path / file_name
        
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        logger.info(f"Streaming {file_path} in chunks of {self.chunk_size}")
        
        # Handle compressed files
        if file_name.endswith('.gz'):
            file_obj = gzip.open(file_path, 'rt', encoding='utf-8', errors='ignore')
        elif file_name.endswith('.zip'):
            with zipfile.ZipFile(file_path) as zf:
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                if csv_files:
                    file_obj = io.TextIOWrapper(
                        zf.open(csv_files[0]),
                        encoding='utf-8',
                        errors='ignore'
                    )
                else:
                    raise ValueError("No CSV files found in zip archive")
        else:
            file_obj = open(file_path, 'r', encoding='utf-8', errors='ignore')
        
        try:
            csv_reader = csv.DictReader(file_obj)
            
            chunk = []
            for row in csv_reader:
                chunk.append(row)
                
                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = []
            
            # Yield remaining rows
            if chunk:
                yield chunk
                
        finally:
            file_obj.close()
    
    def get_sample(self, file_name: str, sample_size: int = 10) -> List[Dict[str, Any]]:
        """
        Get sample rows from local CSV.
        
        Args:
            file_name: Name of CSV file
            sample_size: Number of rows to sample
            
        Returns:
            Sample rows as list of dictionaries
        """
        sample = []
        
        for chunk in self.stream_chunks(file_name):
            for row in chunk:
                sample.append(row)
                if len(sample) >= sample_size:
                    return sample
        
        return sample
    
    def stream_chunk_range(
        self,
        file_name: str,
        start_row: int,
        end_row: int
    ) -> List[Dict[str, Any]]:
        """
        Stream a specific range of rows from local CSV.
        
        Args:
            file_name: Name of CSV file
            start_row: Starting row index (0-based, excluding header)
            end_row: Ending row index (exclusive)
            
        Returns:
            List of rows in the specified range
        """
        logger.debug(f"Streaming rows {start_row} to {end_row} from {file_name}")
        
        result = []
        current_row = 0
        
        for chunk in self.stream_chunks(file_name):
            for row in chunk:
                if current_row >= end_row:
                    return result
                
                if current_row >= start_row:
                    result.append(row)
                
                current_row += 1
        
        return result


def get_csv_streamer(use_local: bool = None) -> CSVStreamer:
    """
    Get appropriate CSV streamer based on configuration.
    
    Args:
        use_local: Override for local/S3 selection
        
    Returns:
        CSVStreamer instance
    """
    if use_local is None:
        use_local = settings.USE_LOCAL_FILES
    
    if use_local:
        return LocalCSVStreamer(
            base_path=settings.LOCAL_CSV_FOLDER,
            chunk_size=settings.CHUNK_SIZE
        )
    else:
        return S3CSVStreamer(
            bucket_name=settings.S3_BUCKET_NAME,
            chunk_size=settings.CHUNK_SIZE
        )


def analyze_csv_structure(file_path: str, use_local: bool = None) -> Dict[str, Any]:
    """
    Analyze CSV file structure and statistics.
    
    Args:
        file_path: Path to CSV file (S3 key or local path)
        use_local: Whether to use local filesystem
        
    Returns:
        Dict with CSV analysis results
    """
    streamer = get_csv_streamer(use_local)
    
    # Get total rows
    total_rows = streamer.count_rows(file_path)
    
    # Get sample for analysis
    sample = streamer.get_sample(file_path, sample_size=100)
    
    if not sample:
        return {
            'total_rows': 0,
            'columns': [],
            'estimated_size_mb': 0
        }
    
    # Analyze columns
    columns = list(sample[0].keys()) if sample else []
    
    # Estimate average row size
    avg_row_size = 0
    for row in sample[:10]:
        row_str = ','.join(str(v) for v in row.values())
        avg_row_size += len(row_str.encode('utf-8'))
    avg_row_size = avg_row_size / min(10, len(sample)) if sample else 0
    
    # Estimate total size
    estimated_size_bytes = avg_row_size * total_rows
    estimated_size_mb = estimated_size_bytes / (1024 * 1024)
    
    return {
        'total_rows': total_rows,
        'columns': columns,
        'column_count': len(columns),
        'estimated_size_mb': round(estimated_size_mb, 2),
        'avg_row_size_bytes': int(avg_row_size),
        'sample_data': sample[:5]  # Return first 5 rows as sample
    }