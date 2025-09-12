import boto3
import csv
import io
import logging
import os
from typing import Dict, Any, List, Iterator, Optional
from datetime import datetime


logger = logging.getLogger(__name__)


class S3CSVClient:
    def __init__(self, bucket_name: Optional[str] = None, chunk_size: int = 1000, 
                 use_local_files: bool = False, local_csv_folder: Optional[str] = None):
        """
        Initialize S3 CSV client for reading large CSV files from S3 or local filesystem.
        
        Args:
            bucket_name (str, optional): S3 bucket name. If not provided, uses environment variable.
            chunk_size (int): Number of rows to process at a time for memory efficiency.
            use_local_files (bool): If True, read from local filesystem instead of S3.
            local_csv_folder (str, optional): Local folder path for CSV files when use_local_files is True.
        """
        self.chunk_size = chunk_size
        self.use_local_files = use_local_files or os.environ.get('USE_LOCAL_FILES', 'false').lower() == 'true'
        
        if self.use_local_files:
            # Local file mode
            self.local_csv_folder = local_csv_folder or os.environ.get('LOCAL_CSV_FOLDER', './test_data/csv_files')
            if not os.path.exists(self.local_csv_folder):
                os.makedirs(self.local_csv_folder, exist_ok=True)
            logger.info(f"S3CSVClient initialized in LOCAL mode with folder: {self.local_csv_folder}, chunk_size: {chunk_size}")
        else:
            # S3 mode
            self.bucket_name = bucket_name or os.environ.get('S3_BUCKET_NAME')
            if not self.bucket_name:
                raise ValueError("Missing S3_BUCKET_NAME environment variable or bucket_name parameter")
            
            # Initialize S3 client
            self.s3_client = boto3.client('s3')
            logger.info(f"S3CSVClient initialized in S3 mode with bucket: {self.bucket_name}, chunk_size: {chunk_size}")
    
    def fetch_csv_data(self, s3_key: str, start_date: str, end_date: str, 
                      date_column: str = 'created_at') -> Iterator[List[Dict[str, Any]]]:
        """
        Fetch and filter CSV data from S3 or local filesystem in chunks for memory efficiency.
        
        Args:
            s3_key (str): S3 object key or local file name for the CSV file
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            date_column (str): Column name containing date for filtering
            
        Yields:
            List[Dict[str, Any]]: Chunks of filtered CSV records
        """
        try:
            # Parse date range for filtering
            start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
            end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
            
            if self.use_local_files:
                # Local file mode
                file_path = os.path.join(self.local_csv_folder, s3_key)
                logger.info(f"Fetching CSV data from local file: {file_path}")
                logger.info(f"Date range: {start_date} to {end_date}, filtering by column: {date_column}")
                
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"Local CSV file not found: {file_path}")
                
                # Get file size for logging
                object_size = os.path.getsize(file_path)
                logger.info(f"CSV file size: {object_size} bytes")
                
                # Open and read local CSV file
                with open(file_path, 'r', encoding='utf-8') as f:
                    csv_reader = csv.DictReader(f)
                    
                    chunk = []
                    total_rows = 0
                    filtered_rows = 0
                    
                    for row in csv_reader:
                        total_rows += 1
                        
                        # Filter by date range if date_column exists
                        if date_column in row and row[date_column]:
                            try:
                                # Try multiple date formats
                                row_date = self._parse_date(row[date_column])
                                if not (start_datetime <= row_date <= end_datetime):
                                    continue
                            except ValueError as e:
                                logger.warning(f"Could not parse date '{row[date_column]}' in row {total_rows}: {e}")
                                continue
                        
                        chunk.append(row)
                        filtered_rows += 1
                        
                        # Yield chunk when it reaches the specified size
                        if len(chunk) >= self.chunk_size:
                            logger.debug(f"Yielding chunk of {len(chunk)} records (total filtered: {filtered_rows})")
                            yield chunk
                            chunk = []
                    
                    # Yield remaining records
                    if chunk:
                        logger.debug(f"Yielding final chunk of {len(chunk)} records")
                        yield chunk
                    
                    logger.info(f"CSV processing completed: {total_rows} total rows, {filtered_rows} filtered rows")
            else:
                # S3 mode
                logger.info(f"Fetching CSV data from s3://{self.bucket_name}/{s3_key}")
                logger.info(f"Date range: {start_date} to {end_date}, filtering by column: {date_column}")
                
                # Get object from S3
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
                
                # Read CSV content
                csv_content = response['Body'].read().decode('utf-8')
                csv_file = io.StringIO(csv_content)
                
                # Create CSV reader
                csv_reader = csv.DictReader(csv_file)
                
                # Get total size for logging
                object_size = response['ContentLength']
                logger.info(f"CSV file size: {object_size} bytes")
                
                chunk = []
                total_rows = 0
                filtered_rows = 0
                
                for row in csv_reader:
                    total_rows += 1
                    
                    # Filter by date range if date_column exists
                    if date_column in row and row[date_column]:
                        try:
                            # Try multiple date formats
                            row_date = self._parse_date(row[date_column])
                            if not (start_datetime <= row_date <= end_datetime):
                                continue
                        except ValueError as e:
                            logger.warning(f"Could not parse date '{row[date_column]}' in row {total_rows}: {e}")
                            continue
                    
                    chunk.append(row)
                    filtered_rows += 1
                    
                    # Yield chunk when it reaches the specified size
                    if len(chunk) >= self.chunk_size:
                        logger.debug(f"Yielding chunk of {len(chunk)} records (total filtered: {filtered_rows})")
                        yield chunk
                        chunk = []
                
                # Yield remaining records
                if chunk:
                    logger.debug(f"Yielding final chunk of {len(chunk)} records")
                    yield chunk
                
                logger.info(f"CSV processing completed: {total_rows} total rows, {filtered_rows} filtered rows")
            
        except Exception as e:
            logger.error(f"Error fetching CSV data from S3: {e}")
            raise
    
    def fetch_all_csv_data(self, s3_key: str, start_date: str, end_date: str, 
                          date_column: str = 'created_at') -> List[Dict[str, Any]]:
        """
        Fetch all CSV data as a single list (for compatibility with existing code).
        Use with caution for large files as it loads everything into memory.
        
        Args:
            s3_key (str): S3 object key for the CSV file
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            date_column (str): Column name containing date for filtering
            
        Returns:
            List[Dict[str, Any]]: All filtered CSV records
        """
        all_data = []
        for chunk in self.fetch_csv_data(s3_key, start_date, end_date, date_column):
            all_data.extend(chunk)
        
        logger.info(f"Loaded {len(all_data)} total records from CSV")
        return all_data
    
    def fetch_entire_csv(self, s3_key: str) -> List[Dict[str, Any]]:
        """
        Fetch entire CSV file without any date filtering.
        Use with caution for large files as it loads everything into memory.
        
        Args:
            s3_key (str): S3 object key for the CSV file
            
        Returns:
            List[Dict[str, Any]]: All CSV records
        """
        try:
            logger.info(f"Fetching entire CSV file from s3://{self.bucket_name}/{s3_key}")
            
            # Get object from S3
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            
            # Read CSV content
            csv_content = response['Body'].read().decode('utf-8')
            csv_file = io.StringIO(csv_content)
            
            # Create CSV reader
            csv_reader = csv.DictReader(csv_file)
            
            # Get total size for logging
            object_size = response['ContentLength']
            logger.info(f"CSV file size: {object_size} bytes")
            
            # Read all rows
            all_data = list(csv_reader)
            
            logger.info(f"CSV processing completed: {len(all_data)} total rows loaded")
            return all_data
            
        except Exception as e:
            logger.error(f"Error fetching entire CSV data from S3: {e}")
            raise
    
    def fetch_csv_data_no_filter(self, s3_key: str) -> Iterator[List[Dict[str, Any]]]:
        """
        Fetch CSV data from S3 in chunks without any date filtering.
        Optimized for large files (100MB+) to avoid memory issues.
        
        Args:
            s3_key (str): S3 object key for the CSV file
            
        Yields:
            List[Dict[str, Any]]: Chunks of CSV records
        """
        try:
            logger.info(f"Fetching large CSV data in chunks from s3://{self.bucket_name}/{s3_key}")
            
            # Get object from S3
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            
            # Get total size for logging
            object_size = response['ContentLength']
            logger.info(f"CSV file size: {object_size} bytes ({object_size / 1024 / 1024:.1f} MB)")
            
            # Read CSV content in streaming fashion
            csv_content = response['Body'].read().decode('utf-8')
            csv_file = io.StringIO(csv_content)
            
            # Create CSV reader
            csv_reader = csv.DictReader(csv_file)
            
            chunk = []
            total_rows = 0
            
            for row in csv_reader:
                total_rows += 1
                chunk.append(row)
                
                # Yield chunk when it reaches the specified size
                if len(chunk) >= self.chunk_size:
                    logger.debug(f"Yielding chunk of {len(chunk)} records (total processed: {total_rows})")
                    yield chunk
                    chunk = []
            
            # Yield remaining records
            if chunk:
                logger.debug(f"Yielding final chunk of {len(chunk)} records")
                yield chunk
            
            logger.info(f"CSV chunked processing completed: {total_rows} total rows processed")
            
        except Exception as e:
            logger.error(f"Error fetching chunked CSV data from S3: {e}")
            raise
    
    def fetch_csv_chunk_by_rows(self, s3_key: str, start_row: int, end_row: int) -> List[Dict[str, Any]]:
        """
        Fetch specific rows from CSV file efficiently for Step Functions processing.
        Optimized for row-based chunking without loading entire file.
        
        Args:
            s3_key (str): S3 object key or local file name for the CSV file
            start_row (int): Starting row number (1-based, excluding header)
            end_row (int): Ending row number (1-based, inclusive)
            
        Returns:
            List[Dict[str, Any]]: CSV records for the specified row range
        """
        try:
            if self.use_local_files:
                # Local file mode
                file_path = os.path.join(self.local_csv_folder, s3_key)
                logger.info(f"Fetching CSV rows {start_row}-{end_row} from local file: {file_path}")
                
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"Local CSV file not found: {file_path}")
                
                chunk_data = []
                with open(file_path, 'r', encoding='utf-8') as f:
                    csv_reader = csv.DictReader(f)
                    
                    current_row = 1  # Start counting from 1 (header is row 0)
                    for row in csv_reader:
                        if current_row >= start_row and current_row <= end_row:
                            chunk_data.append(row)
                        elif current_row > end_row:
                            break  # No need to continue reading
                        current_row += 1
                
                logger.info(f"Successfully fetched {len(chunk_data)} rows from range {start_row}-{end_row}")
                return chunk_data
            else:
                # S3 mode
                logger.info(f"Fetching CSV rows {start_row}-{end_row} from s3://{self.bucket_name}/{s3_key}")
            
            # Get object from S3
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            
            # Read CSV content
            csv_content = response['Body'].read().decode('utf-8')
            csv_file = io.StringIO(csv_content)
            
            # Create CSV reader
            csv_reader = csv.DictReader(csv_file)
            
            chunk_data = []
            current_row = 1  # Start counting from 1 (header is row 0, data starts at row 1)
            
            for row in csv_reader:
                if current_row >= start_row and current_row <= end_row:
                    chunk_data.append(row)
                elif current_row > end_row:
                    break  # No need to continue reading
                current_row += 1
            
            logger.info(f"Successfully fetched {len(chunk_data)} rows from range {start_row}-{end_row}")
            return chunk_data
            
        except Exception as e:
            logger.error(f"Error fetching CSV chunk rows {start_row}-{end_row}: {e}")
            raise
    
    def count_csv_rows(self, s3_key: str) -> int:
        """
        Count exact number of rows in CSV file.
        
        Args:
            s3_key (str): S3 object key or local file name for the CSV file
            
        Returns:
            int: Exact number of data rows (excluding header)
        """
        try:
            if self.use_local_files:
                # Local file mode
                file_path = os.path.join(self.local_csv_folder, s3_key)
                logger.info(f"Counting rows in local CSV file: {file_path}")
                
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"Local CSV file not found: {file_path}")
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    # Skip header and count data rows
                    next(f)  # Skip header
                    row_count = sum(1 for line in f if line.strip())
                
                logger.info(f"CSV file has {row_count} data rows")
                return row_count
            else:
                # For S3, use estimate for now (full count would require downloading entire file)
                return self.estimate_csv_rows(s3_key)
                
        except Exception as e:
            logger.error(f"Error counting CSV rows: {e}")
            # Return estimate as fallback
            return self.estimate_csv_rows(s3_key)
    
    def estimate_csv_rows(self, s3_key: str, sample_size: int = 10240) -> int:
        """
        Estimate total number of rows in CSV file by analyzing a sample.
        
        Args:
            s3_key (str): S3 object key for the CSV file
            sample_size (int): Size of sample to read in bytes
            
        Returns:
            int: Estimated total number of rows (excluding header)
        """
        try:
            # Get file size
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            total_file_size = response['ContentLength']
            
            # Read sample from beginning of file
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, 
                Key=s3_key,
                Range=f'bytes=0-{sample_size-1}'
            )
            
            sample_content = response['Body'].read().decode('utf-8')
            sample_lines = sample_content.split('\n')
            
            # Count non-empty lines in sample (excluding header)
            sample_data_lines = len([line for line in sample_lines[1:] if line.strip()])
            
            if sample_data_lines > 0:
                # Calculate average bytes per data row
                sample_data_content = '\n'.join(sample_lines[1:])  # Exclude header
                avg_bytes_per_row = len(sample_data_content.encode('utf-8')) / sample_data_lines
                
                # Estimate total data rows
                header_size = len(sample_lines[0].encode('utf-8')) if sample_lines else 0
                data_content_size = total_file_size - header_size
                estimated_rows = max(1, int(data_content_size / avg_bytes_per_row))
            else:
                estimated_rows = 1  # Fallback
            
            logger.info(f"Estimated {estimated_rows} data rows in CSV file ({total_file_size / 1024 / 1024:.1f} MB)")
            return estimated_rows
            
        except Exception as e:
            logger.error(f"Error estimating CSV rows: {e}")
            return 1000  # Conservative fallback estimate
    
    def _parse_date(self, date_string: str) -> datetime:
        """
        Parse date string with multiple format support.
        
        Args:
            date_string (str): Date string to parse
            
        Returns:
            datetime: Parsed datetime object
        """
        # Common date formats to try
        formats = [
            '%Y-%m-%d',           # 2023-12-25
            '%Y-%m-%d %H:%M:%S',  # 2023-12-25 10:30:00
            '%Y/%m/%d',           # 2023/12/25
            '%m/%d/%Y',           # 12/25/2023
            '%d/%m/%Y',           # 25/12/2023
            '%Y-%m-%dT%H:%M:%S',  # 2023-12-25T10:30:00
            '%Y-%m-%dT%H:%M:%SZ', # 2023-12-25T10:30:00Z
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_string.strip(), fmt)
            except ValueError:
                continue
        
        raise ValueError(f"Unable to parse date: {date_string}")
    
    def validate_csv_structure(self, s3_key: str, required_columns: List[str]) -> Dict[str, Any]:
        """
        Validate CSV structure and return metadata.
        
        Args:
            s3_key (str): S3 object key for the CSV file
            required_columns (List[str]): Required column names
            
        Returns:
            Dict[str, Any]: Validation results and metadata
        """
        try:
            # Get object metadata
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            
            # Read first few lines to check structure
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, 
                Key=s3_key,
                Range='bytes=0-1024'  # Read first 1KB
            )
            
            csv_content = response['Body'].read().decode('utf-8')
            csv_file = io.StringIO(csv_content)
            csv_reader = csv.DictReader(csv_file)
            
            # Get column names
            fieldnames = csv_reader.fieldnames or []
            
            # Check for required columns
            missing_columns = [col for col in required_columns if col not in fieldnames]
            
            return {
                'valid': len(missing_columns) == 0,
                'columns': fieldnames,
                'missing_columns': missing_columns,
                'file_size': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified')
            }
            
        except Exception as e:
            logger.error(f"Error validating CSV structure: {e}")
            return {
                'valid': False,
                'error': str(e),
                'columns': [],
                'missing_columns': required_columns
            }