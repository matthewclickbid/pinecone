import json
import logging
import os
import boto3
import csv
import io
from typing import Dict, Any, List
from services.dynamodb_client import DynamoDBClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    CSV Initializer Lambda for Step Functions workflow.
    Analyzes CSV file and creates processing chunks.
    
    Args:
        event: Contains task_id, s3_key, question_id, bucket_name
        context: Lambda context
        
    Returns:
        Dict: Initialization result with chunks array
    """
    try:
        # Extract parameters
        task_id = event['task_id']
        s3_key = event['s3_key']
        question_id = event['question_id']
        bucket_name = event['bucket_name']
        start_date = event.get('start_date')
        end_date = event.get('end_date')
        
        logger.info(f"Initializing CSV processing for task {task_id}")
        logger.info(f"S3 location: s3://{bucket_name}/{s3_key}")
        logger.info(f"Question ID: {question_id}")
        
        # Initialize clients
        s3_client = boto3.client('s3')
        dynamodb_client = DynamoDBClient()
        
        # Get CSV file info and validate structure
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            file_size = response['ContentLength']
            file_size_mb = file_size / (1024 * 1024)
            
            logger.info(f"CSV file size: {file_size_mb:.1f} MB ({file_size} bytes)")
            
        except Exception as e:
            error_msg = f"Failed to access S3 file: {e}"
            logger.error(error_msg)
            dynamodb_client.set_task_error(task_id, error_msg)
            return create_error_response(error_msg)
        
        # Read CSV header and estimate total rows
        try:
            # Read first part of file to analyze structure
            response = s3_client.get_object(
                Bucket=bucket_name, 
                Key=s3_key,
                Range='bytes=0-10240'  # Read first 10KB
            )
            
            sample_content = response['Body'].read().decode('utf-8')
            sample_lines = sample_content.split('\n')
            
            # Parse CSV header
            if not sample_lines:
                raise ValueError("CSV file appears to be empty")
                
            csv_reader = csv.DictReader(io.StringIO(sample_content))
            fieldnames = csv_reader.fieldnames or []
            
            # Validate required columns
            required_columns = ['id', 'formatted_text']
            missing_columns = [col for col in required_columns if col not in fieldnames]
            
            if missing_columns:
                error_msg = f"CSV missing required columns: {missing_columns}. Available columns: {fieldnames}"
                logger.error(error_msg)
                dynamodb_client.set_task_error(task_id, error_msg)
                return create_error_response(error_msg)
            
            logger.info(f"CSV columns validated: {fieldnames}")
            
            # Estimate total rows based on file size and average row length
            sample_row_count = len([line for line in sample_lines if line.strip()])
            if sample_row_count > 1:  # Account for header
                avg_bytes_per_row = len(sample_content.encode('utf-8')) / sample_row_count
                estimated_total_rows = max(1, int(file_size / avg_bytes_per_row) - 1)  # Subtract header
            else:
                estimated_total_rows = 1000  # Fallback estimate
            
            logger.info(f"Estimated total rows: {estimated_total_rows}")
            
        except Exception as e:
            error_msg = f"Failed to analyze CSV structure: {e}"
            logger.error(error_msg)
            dynamodb_client.set_task_error(task_id, error_msg)
            return create_error_response(error_msg)
        
        # Calculate optimal chunks
        target_rows_per_chunk = 3500  # Target ~10 minutes processing per chunk
        
        chunks = []
        current_row = 2  # Start after header (row 1)
        chunk_id = 1
        
        while current_row <= estimated_total_rows:
            chunk_end = min(current_row + target_rows_per_chunk - 1, estimated_total_rows)
            
            chunk = {
                "task_id": task_id,
                "chunk_id": f"chunk_{chunk_id}",
                "s3_key": s3_key,
                "bucket_name": bucket_name,
                "start_row": current_row,
                "end_row": chunk_end,
                "question_id": question_id,
                "expected_records": chunk_end - current_row + 1
            }
            
            chunks.append(chunk)
            logger.info(f"Created {chunk['chunk_id']}: rows {current_row}-{chunk_end} ({chunk['expected_records']} records)")
            
            current_row = chunk_end + 1
            chunk_id += 1
        
        total_chunks = len(chunks)
        logger.info(f"Created {total_chunks} chunks for processing")
        
        # Update DynamoDB with chunk information
        try:
            dynamodb_client.update_task_with_chunks(
                task_id=task_id,
                total_chunks=total_chunks,
                estimated_total_records=estimated_total_rows,
                chunks_metadata=chunks
            )
            logger.info(f"Updated DynamoDB with chunk information")
            
        except Exception as e:
            error_msg = f"Failed to update DynamoDB with chunks: {e}"
            logger.error(error_msg)
            return create_error_response(error_msg)
        
        # Return successful initialization
        return {
            "success": True,
            "task_id": task_id,
            "total_chunks": total_chunks,
            "estimated_total_records": estimated_total_rows,
            "file_size_mb": file_size_mb,
            "chunks": chunks,
            "csv_columns": fieldnames
        }
        
    except Exception as e:
        error_msg = f"CSV initialization failed: {e}"
        logger.error(error_msg, exc_info=True)
        
        # Try to update task status if possible
        try:
            if 'task_id' in locals():
                dynamodb_client.set_task_error(task_id, error_msg)
        except:
            pass
            
        return create_error_response(error_msg)


def create_error_response(error_message: str) -> Dict[str, Any]:
    """Create standardized error response."""
    return {
        "success": False,
        "error": error_message,
        "chunks": []
    }