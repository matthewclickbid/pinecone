import os
import uuid
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class DynamoDBClient:
    def __init__(self):
        self.table_name = os.environ.get('DYNAMODB_TABLE_NAME', 'vectordb-tasks')
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(self.table_name)
        
        logger.info(f"Initialized DynamoDB client for table: {self.table_name}")
        
        # Lock table for preventing concurrent processing of same date ranges
        self.lock_table_name = f"{self.table_name}-locks"
        self.lock_table = self.dynamodb.Table(self.lock_table_name)
    
    def _convert_decimal(self, obj):
        """Convert Decimal objects to native Python types for JSON serialization."""
        if isinstance(obj, list):
            return [self._convert_decimal(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._convert_decimal(value) for key, value in obj.items()}
        elif isinstance(obj, Decimal):
            # Convert Decimal to int if it's a whole number, otherwise to float
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        else:
            return obj
    
    def _convert_to_dynamodb_format(self, obj):
        """Convert Python types to DynamoDB-compatible formats."""
        if isinstance(obj, float):
            # Convert float to Decimal for DynamoDB
            return Decimal(str(obj))
        elif isinstance(obj, list):
            return [self._convert_to_dynamodb_format(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._convert_to_dynamodb_format(value) for key, value in obj.items()}
        else:
            return obj
    
    def create_task(self, start_date: str, end_date: str, total_records: int = 0, task_type: str = 'data_processing') -> str:
        """
        Create a new task in DynamoDB.
        
        Args:
            start_date (str): Start date for the task
            end_date (str): End date for the task
            total_records (int): Total number of records to process
            task_type (str): Type of task (data_processing, namespace_migration)
            
        Returns:
            str: Task ID
        """
        try:
            task_id = str(uuid.uuid4())
            timestamp = datetime.utcnow().isoformat()
            
            item = {
                'task_id': task_id,
                'status': 'PENDING',
                'task_type': task_type,
                'start_date': start_date,
                'end_date': end_date,
                'total_records': total_records,
                'processed_records': 0,
                'failed_records': 0,
                'created_at': timestamp,
                'updated_at': timestamp,
                'error_message': None
            }
            
            self.table.put_item(Item=item)
            logger.info(f"Created task {task_id} with status PENDING")
            
            return task_id
            
        except ClientError as e:
            logger.error(f"Error creating task: {e}")
            raise
    
    def update_task_status(self, task_id: str, status: str, **kwargs) -> None:
        """
        Update task status and other fields.
        
        Args:
            task_id (str): Task ID
            status (str): New status (PENDING, IN_PROGRESS, COMPLETED, FAILED)
            **kwargs: Additional fields to update
        """
        try:
            update_expression = "SET #status = :status, updated_at = :updated_at"
            expression_attribute_names = {'#status': 'status'}
            expression_attribute_values = {
                ':status': status,
                ':updated_at': datetime.utcnow().isoformat()
            }
            
            # Add additional fields to update
            for key, value in kwargs.items():
                if key not in ['task_id']:  # Skip primary key
                    update_expression += f", {key} = :{key}"
                    expression_attribute_values[f":{key}"] = self._convert_to_dynamodb_format(value)
            
            self.table.update_item(
                Key={'task_id': task_id},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
            
            logger.info(f"Updated task {task_id} status to {status}")
            
        except ClientError as e:
            logger.error(f"Error updating task {task_id}: {e}")
            raise
    
    def increment_processed_records(self, task_id: str, increment: int = 1) -> None:
        """
        Increment the processed records count for a task.
        
        Args:
            task_id (str): Task ID
            increment (int): Number to increment by
        """
        try:
            self.table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="ADD processed_records :inc SET updated_at = :updated_at",
                ExpressionAttributeValues={
                    ':inc': increment,
                    ':updated_at': datetime.utcnow().isoformat()
                }
            )
            
            logger.debug(f"Incremented processed records for task {task_id} by {increment}")
            
        except ClientError as e:
            logger.error(f"Error incrementing processed records for task {task_id}: {e}")
            raise
    
    def increment_failed_records(self, task_id: str, increment: int = 1) -> None:
        """
        Increment the failed records count for a task.
        
        Args:
            task_id (str): Task ID
            increment (int): Number to increment by
        """
        try:
            self.table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="ADD failed_records :inc SET updated_at = :updated_at",
                ExpressionAttributeValues={
                    ':inc': increment,
                    ':updated_at': datetime.utcnow().isoformat()
                }
            )
            
            logger.debug(f"Incremented failed records for task {task_id} by {increment}")
            
        except ClientError as e:
            logger.error(f"Error incrementing failed records for task {task_id}: {e}")
            raise
    
    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get task details by task ID.
        
        Args:
            task_id (str): Task ID
            
        Returns:
            Optional[Dict]: Task details or None if not found
        """
        try:
            response = self.table.get_item(Key={'task_id': task_id})
            
            if 'Item' in response:
                # Convert Decimal objects to native Python types
                return self._convert_decimal(response['Item'])
            else:
                logger.warning(f"Task {task_id} not found")
                return None
                
        except ClientError as e:
            logger.error(f"Error getting task {task_id}: {e}")
            raise
    
    def set_task_error(self, task_id: str, error_message: str) -> None:
        """
        Set task status to FAILED with error message.
        
        Args:
            task_id (str): Task ID
            error_message (str): Error message
        """
        try:
            self.update_task_status(task_id, 'FAILED', error_message=error_message)
            logger.error(f"Task {task_id} failed: {error_message}")
            
        except ClientError as e:
            logger.error(f"Error setting task error for {task_id}: {e}")
            raise
    
    def update_task_with_chunks(self, task_id: str, total_chunks: int, estimated_total_records: int, chunks_metadata: list) -> None:
        """
        Update task with Step Functions chunk information.
        
        Args:
            task_id: Task identifier
            total_chunks: Total number of chunks to process
            estimated_total_records: Estimated total records in CSV
            chunks_metadata: List of chunk metadata
        """
        try:
            current_time = datetime.utcnow().isoformat()
            
            # Create chunk status mapping
            chunk_status = {}
            chunk_processed = {}
            chunk_failed = {}
            chunk_vectors = {}
            chunk_errors = {}
            
            for chunk in chunks_metadata:
                chunk_id = chunk['chunk_id']
                chunk_status[chunk_id] = 'PENDING'
                # Initialize other maps as empty for each chunk
                # These will be populated when chunks are processed
            
            self.table.update_item(
                Key={'task_id': task_id},
                UpdateExpression='SET #status = :status, updated_at = :updated_at, total_chunks = :total_chunks, estimated_total_records = :estimated_total_records, completed_chunks = :completed_chunks, failed_chunks = :failed_chunks, chunk_status = :chunk_status, chunk_processed = :chunk_processed, chunk_failed = :chunk_failed, chunk_vectors = :chunk_vectors, chunk_errors = :chunk_errors',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={
                    ':status': 'INITIALIZING',
                    ':updated_at': current_time,
                    ':total_chunks': total_chunks,
                    ':estimated_total_records': estimated_total_records,
                    ':completed_chunks': 0,
                    ':failed_chunks': 0,
                    ':chunk_status': chunk_status,
                    ':chunk_processed': chunk_processed,
                    ':chunk_failed': chunk_failed,
                    ':chunk_vectors': chunk_vectors,
                    ':chunk_errors': chunk_errors
                }
            )
            
            logger.info(f"Updated task {task_id} with {total_chunks} chunks")
            
        except ClientError as e:
            logger.error(f"Error updating task with chunks for {task_id}: {e}")
            raise
    
    def update_chunk_status(self, task_id: str, chunk_id: str, status: str, error_message: str = None, 
                           processed_records: int = None, failed_records: int = None, vectors_upserted: int = None) -> None:
        """
        Update the status of a specific chunk.
        
        Args:
            task_id: Task identifier
            chunk_id: Chunk identifier
            status: New chunk status (PENDING, IN_PROGRESS, COMPLETED, FAILED)
            error_message: Optional error message for failed chunks
            processed_records: Number of records processed in this chunk
            failed_records: Number of records that failed in this chunk
            vectors_upserted: Number of vectors upserted to Pinecone
        """
        try:
            current_time = datetime.utcnow().isoformat()
            
            # Build update expression dynamically
            # Start with basic updates that always happen
            update_expression_parts = [
                'chunk_status.#chunk_id = :status',
                'updated_at = :updated_at'
            ]
            
            expression_attribute_names = {'#chunk_id': chunk_id}
            expression_attribute_values = {
                ':status': status,
                ':updated_at': current_time
            }
            
            # Initialize all maps that need to exist
            map_inits = []
            if error_message:
                map_inits.append('chunk_errors = if_not_exists(chunk_errors, :empty_map)')
            if processed_records is not None:
                map_inits.append('chunk_processed = if_not_exists(chunk_processed, :empty_map)')
            if failed_records is not None:
                map_inits.append('chunk_failed = if_not_exists(chunk_failed, :empty_map)')
            if vectors_upserted is not None:
                map_inits.append('chunk_vectors = if_not_exists(chunk_vectors, :empty_map)')
            
            # Only add :empty_map if we have any map initializations
            if map_inits:
                expression_attribute_values[':empty_map'] = {}
            
            # Add map initializations first
            update_expression_parts.extend(map_inits)
            
            # Now add the specific field updates
            if error_message:
                update_expression_parts.append('chunk_errors.#chunk_id = :error')
                expression_attribute_values[':error'] = error_message
            
            if processed_records is not None:
                update_expression_parts.append('chunk_processed.#chunk_id = :processed')
                expression_attribute_values[':processed'] = processed_records
            
            if failed_records is not None:
                update_expression_parts.append('chunk_failed.#chunk_id = :failed')
                expression_attribute_values[':failed'] = failed_records
            
            if vectors_upserted is not None:
                update_expression_parts.append('chunk_vectors.#chunk_id = :vectors')
                expression_attribute_values[':vectors'] = vectors_upserted
            
            # Combine all parts into final expression
            update_expression = 'SET ' + ', '.join(update_expression_parts)
            
            self.table.update_item(
                Key={'task_id': task_id},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
            
            logger.debug(f"Updated chunk {chunk_id} status to {status} for task {task_id}")
            
        except ClientError as e:
            logger.error(f"Error updating chunk {chunk_id} status for {task_id}: {e}")
            # Don't raise here - chunk status update failures shouldn't fail the entire process

    def acquire_processing_lock(self, start_date: str, end_date: str, lambda_request_id: str) -> bool:
        """
        Acquire a lock for processing a specific date range to prevent concurrent processing.
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            lambda_request_id (str): Lambda request ID for this invocation
            
        Returns:
            bool: True if lock acquired successfully, False if already locked
        """
        try:
            lock_key = f"{start_date}_{end_date}"
            current_time = datetime.utcnow().isoformat()
            
            # Try to acquire lock with conditional write
            self.lock_table.put_item(
                Item={
                    'lock_key': lock_key,
                    'lambda_request_id': lambda_request_id,
                    'acquired_at': current_time,
                    'start_date': start_date,
                    'end_date': end_date,
                    'ttl': int(datetime.utcnow().timestamp()) + 3600  # Lock expires in 1 hour
                },
                ConditionExpression='attribute_not_exists(lock_key)'
            )
            
            logger.info(f"Successfully acquired processing lock for {start_date} to {end_date} (request: {lambda_request_id})")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                # Lock already exists
                logger.warning(f"Processing lock already exists for {start_date} to {end_date}")
                try:
                    # Get existing lock info
                    response = self.lock_table.get_item(Key={'lock_key': lock_key})
                    if 'Item' in response:
                        existing_lock = response['Item']
                        logger.warning(f"Existing lock held by request {existing_lock.get('lambda_request_id')} since {existing_lock.get('acquired_at')}")
                except:
                    pass
                return False
            else:
                logger.error(f"Error acquiring processing lock: {e}")
                raise

    def release_processing_lock(self, start_date: str, end_date: str, lambda_request_id: str) -> None:
        """
        Release the processing lock for a specific date range.
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            lambda_request_id (str): Lambda request ID that holds the lock
        """
        try:
            lock_key = f"{start_date}_{end_date}"
            
            # Only delete if we own the lock
            self.lock_table.delete_item(
                Key={'lock_key': lock_key},
                ConditionExpression='lambda_request_id = :request_id',
                ExpressionAttributeValues={':request_id': lambda_request_id}
            )
            
            logger.info(f"Successfully released processing lock for {start_date} to {end_date} (request: {lambda_request_id})")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                logger.warning(f"Cannot release lock - not owned by request {lambda_request_id}")
            else:
                logger.error(f"Error releasing processing lock: {e}")
                # Don't raise - this is cleanup, shouldn't fail the main process
    
    def update_task_metadata(self, task_id: str, metadata: Dict[str, Any]) -> None:
        """
        Update task with metadata fields (for migration tracking).
        
        Args:
            task_id (str): Task ID
            metadata (Dict): Metadata fields to update
        """
        try:
            update_expression_parts = []
            expression_attribute_names = {}
            expression_attribute_values = {
                ':updated_at': datetime.utcnow().isoformat()
            }
            
            for key, value in metadata.items():
                if key == 'task_id':  # Skip primary key
                    continue
                
                # Handle nested fields (e.g., migration_progress.phase)
                if '.' in key:
                    parts = key.split('.')
                    safe_key = parts[0]
                    nested_path = '.'.join(f'#{p}' for p in parts[1:])
                    full_path = f'#{safe_key}.{nested_path}'
                    
                    for part in parts:
                        expression_attribute_names[f'#{part}'] = part
                else:
                    safe_key = f'#{key}'
                    full_path = safe_key
                    expression_attribute_names[safe_key] = key
                
                value_key = f':{key.replace(".", "_")}'
                update_expression_parts.append(f'{full_path} = {value_key}')
                expression_attribute_values[value_key] = self._convert_to_dynamodb_format(value)
            
            if not update_expression_parts:
                return
            
            update_expression = 'SET ' + ', '.join(update_expression_parts) + ', updated_at = :updated_at'
            
            self.table.update_item(
                Key={'task_id': task_id},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names if expression_attribute_names else None,
                ExpressionAttributeValues=expression_attribute_values
            )
            
            logger.debug(f"Updated task {task_id} metadata: {list(metadata.keys())}")
            
        except ClientError as e:
            logger.error(f"Error updating task metadata for {task_id}: {e}")
            raise
    
    def get_migration_tasks(self, status: Optional[str] = None) -> list:
        """
        Get all migration tasks, optionally filtered by status.
        
        Args:
            status (str): Optional status filter
            
        Returns:
            list: List of migration tasks
        """
        try:
            if status:
                response = self.table.scan(
                    FilterExpression='task_type = :type AND #status = :status',
                    ExpressionAttributeNames={'#status': 'status'},
                    ExpressionAttributeValues={
                        ':type': 'namespace_migration',
                        ':status': status
                    }
                )
            else:
                response = self.table.scan(
                    FilterExpression='task_type = :type',
                    ExpressionAttributeValues={':type': 'namespace_migration'}
                )
            
            tasks = response.get('Items', [])
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                if status:
                    response = self.table.scan(
                        FilterExpression='task_type = :type AND #status = :status',
                        ExpressionAttributeNames={'#status': 'status'},
                        ExpressionAttributeValues={
                            ':type': 'namespace_migration',
                            ':status': status
                        },
                        ExclusiveStartKey=response['LastEvaluatedKey']
                    )
                else:
                    response = self.table.scan(
                        FilterExpression='task_type = :type',
                        ExpressionAttributeValues={':type': 'namespace_migration'},
                        ExclusiveStartKey=response['LastEvaluatedKey']
                    )
                tasks.extend(response.get('Items', []))
            
            # Convert Decimal objects
            return [self._convert_decimal(task) for task in tasks]
            
        except ClientError as e:
            logger.error(f"Error getting migration tasks: {e}")
            raise