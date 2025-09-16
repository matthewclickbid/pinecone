import os
import uuid
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from decimal import Decimal

logger = logging.getLogger(__name__)

# Check if we're in local mode
USE_LOCAL_FILES = os.environ.get('USE_LOCAL_FILES', 'false').lower() == 'true'
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'development').lower()

if USE_LOCAL_FILES or ENVIRONMENT == 'local':
    # Use local storage for development/testing
    from app.services.local_storage import LocalStorageClient
    
    class DynamoDBClient(LocalStorageClient):
        """Use LocalStorageClient as DynamoDBClient in local mode"""
        pass
else:
    # Use actual DynamoDB in non-local environments
    import boto3
    from botocore.exceptions import ClientError

    class DynamoDBClient:
        def __init__(self):
            self.table_name = os.environ.get('DYNAMODB_TABLE_NAME', 'vectordb-tasks')
            self.dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
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
        
        def create_background_task(self, task_type: str, parameters: Dict[str, Any], 
                                 priority: str = 'normal', celery_task_id: str = None) -> str:
            """
            Create a background task for Celery processing.
            
            Args:
                task_type: Type of background task (metabase_processing, s3_csv_processing, etc.)
                parameters: Task parameters and configuration
                priority: Task priority (low, normal, high)
                celery_task_id: Celery task ID for tracking
                
            Returns:
                str: Task ID
            """
            try:
                task_id = str(uuid.uuid4())
                timestamp = datetime.utcnow().isoformat()
                
                item = {
                    'task_id': task_id,
                    'status': 'QUEUED',
                    'task_type': task_type,
                    'priority': priority,
                    'parameters': parameters,
                    'celery_task_id': celery_task_id,
                    'created_at': timestamp,
                    'updated_at': timestamp,
                    'queued_at': timestamp,
                    'total_records': 0,
                    'processed_records': 0,
                    'failed_records': 0,
                    'error_message': None,
                    'progress_percentage': 0,
                    'estimated_completion_time': None
                }
                
                self.table.put_item(Item=item)
                logger.info(f"Created background task {task_id} with type {task_type}")
                
                return task_id
                
            except ClientError as e:
                logger.error(f"Error creating background task: {e}")
                raise
        
        def update_task_progress(self, task_id: str, processed_records: int, 
                               total_records: int = None, status: str = None, 
                               metadata: Dict[str, Any] = None) -> None:
            """
            Update task progress with detailed tracking.
            
            Args:
                task_id: Task ID
                processed_records: Current number of processed records
                total_records: Total records to process (optional)
                status: Updated status (optional)
                metadata: Additional metadata to store
            """
            try:
                current_time = datetime.utcnow().isoformat()
                
                update_expression_parts = ['updated_at = :updated_at', 'processed_records = :processed']
                expression_attribute_values = {
                    ':updated_at': current_time,
                    ':processed': processed_records
                }
                
                if total_records is not None:
                    update_expression_parts.append('total_records = :total')
                    expression_attribute_values[':total'] = total_records
                    
                    # Calculate progress percentage
                    progress = (processed_records / total_records) * 100 if total_records > 0 else 0
                    update_expression_parts.append('progress_percentage = :progress')
                    expression_attribute_values[':progress'] = self._convert_to_dynamodb_format(progress)
                
                if status:
                    update_expression_parts.append('#status = :status')
                    expression_attribute_values[':status'] = status
                    
                    # Track status change timestamps
                    if status == 'IN_PROGRESS':
                        update_expression_parts.append('started_at = :started_at')
                        expression_attribute_values[':started_at'] = current_time
                    elif status in ['COMPLETED', 'FAILED', 'CANCELLED']:
                        update_expression_parts.append('completed_at = :completed_at')
                        expression_attribute_values[':completed_at'] = current_time
                
                if metadata:
                    for key, value in metadata.items():
                        if key not in ['task_id']:  # Skip primary key
                            update_expression_parts.append(f'{key} = :{key}')
                            expression_attribute_values[f':{key}'] = self._convert_to_dynamodb_format(value)
                
                update_expression = 'SET ' + ', '.join(update_expression_parts)
                expression_attribute_names = {'#status': 'status'} if status else None
                
                self.table.update_item(
                    Key={'task_id': task_id},
                    UpdateExpression=update_expression,
                    ExpressionAttributeNames=expression_attribute_names,
                    ExpressionAttributeValues=expression_attribute_values
                )
                
                logger.debug(f"Updated task {task_id} progress: {processed_records} processed")
                
            except ClientError as e:
                logger.error(f"Error updating task progress for {task_id}: {e}")
                raise
        
        def get_tasks_by_status(self, status: str, task_type: str = None, limit: int = 100) -> List[Dict[str, Any]]:
            """
            Get tasks by status, optionally filtered by type.
            
            Args:
                status: Task status to filter by
                task_type: Optional task type filter
                limit: Maximum number of tasks to return
                
            Returns:
                List of task records
            """
            try:
                if task_type:
                    response = self.table.scan(
                        FilterExpression='#status = :status AND task_type = :task_type',
                        ExpressionAttributeNames={'#status': 'status'},
                        ExpressionAttributeValues={
                            ':status': status,
                            ':task_type': task_type
                        },
                        Limit=limit
                    )
                else:
                    response = self.table.scan(
                        FilterExpression='#status = :status',
                        ExpressionAttributeNames={'#status': 'status'},
                        ExpressionAttributeValues={':status': status},
                        Limit=limit
                    )
                
                tasks = response.get('Items', [])
                return [self._convert_decimal(task) for task in tasks]
                
            except ClientError as e:
                logger.error(f"Error getting tasks by status {status}: {e}")
                raise
        
        def get_active_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
            """
            Get all active (non-completed) tasks.

            Args:
                limit: Maximum number of tasks to return

            Returns:
                List of active task records
            """
            try:
                response = self.table.scan(
                    FilterExpression='#status IN (:queued, :pending, :in_progress, :processing_chunks, :initializing)',
                    ExpressionAttributeNames={'#status': 'status'},
                    ExpressionAttributeValues={
                        ':queued': 'QUEUED',
                        ':pending': 'PENDING',
                        ':in_progress': 'IN_PROGRESS',
                        ':processing_chunks': 'PROCESSING_CHUNKS',
                        ':initializing': 'INITIALIZING'
                    },
                    Limit=limit
                )

                tasks = response.get('Items', [])
                return [self._convert_decimal(task) for task in tasks]

            except ClientError as e:
                logger.error(f"Error getting active tasks: {e}")
                raise

        def get_all_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
            """
            Get all tasks regardless of status.

            Args:
                limit: Maximum number of tasks to return

            Returns:
                List of all task records
            """
            try:
                tasks = []
                last_evaluated_key = None

                # Paginate through results until we have enough or no more data
                while len(tasks) < limit:
                    scan_params = {}
                    if last_evaluated_key:
                        scan_params['ExclusiveStartKey'] = last_evaluated_key

                    response = self.table.scan(**scan_params)
                    items = response.get('Items', [])
                    tasks.extend(items)

                    # Check if there are more items to scan
                    last_evaluated_key = response.get('LastEvaluatedKey')
                    if not last_evaluated_key:
                        break

                # Truncate to limit if we got more
                tasks = tasks[:limit]

                # Sort by created_at in descending order (newest first)
                tasks.sort(key=lambda x: x.get('created_at', ''), reverse=True)

                return [self._convert_decimal(task) for task in tasks]

            except ClientError as e:
                logger.error(f"Error getting all tasks: {e}")
                raise
        
        # Add stub methods for compatibility
        def increment_processed_records(self, task_id: str, increment: int = 1) -> None:
            """Increment the processed records count for a task."""
            task = self.get_task(task_id)
            if task:
                current = task.get('processed_records', 0)
                self.update_task_progress(task_id, current + increment)
        
        def increment_failed_records(self, task_id: str, increment: int = 1) -> None:
            """Increment the failed records count for a task."""
            task = self.get_task(task_id)
            if task:
                current = task.get('failed_records', 0)
                self.update_task_status(task_id, task.get('status', 'IN_PROGRESS'), 
                                      failed_records=current + increment)
        
        def set_task_error(self, task_id: str, error_message: str) -> None:
            """Set task status to FAILED with error message."""
            self.update_task_status(task_id, 'FAILED', error_message=error_message)
            logger.error(f"Task {task_id} failed: {error_message}")
        
        def cancel_task(self, task_id: str, reason: str = None) -> None:
            """Cancel a task and update its status."""
            metadata = {'cancellation_reason': reason} if reason else {}
            self.update_task_status(task_id, status='CANCELLED', metadata=metadata)
            logger.info(f"Task {task_id} cancelled. Reason: {reason or 'No reason provided'}")
        
        def get_task_statistics(self, time_period_hours: int = 24) -> Dict[str, Any]:
            """Get task statistics for a given time period."""
            # Simplified version for initial implementation
            return {
                'time_period_hours': time_period_hours,
                'total_tasks': 0,
                'status_counts': {},
                'type_counts': {},
                'total_records_processed': 0,
                'total_records_failed': 0,
                'average_processing_time_minutes': 0,
                'success_rate_percentage': 0
            }
        
        def cleanup_completed_tasks(self, retention_days: int = 7) -> int:
            """Clean up completed tasks older than retention period."""
            # Simplified version for initial implementation
            logger.info("Task cleanup not implemented in basic DynamoDB client")
            return 0
        
        # Chunk processing methods for parallel CSV processing
        def update_task_with_chunks(self, task_id: str, total_chunks: int = None,
                                   estimated_total_records: int = None,
                                   chunks_metadata: List[Dict[str, Any]] = None,
                                   chunks: List[Dict[str, Any]] = None) -> None:
            """Update task with chunk information for parallel processing."""
            try:
                # Handle both old and new parameter formats
                chunks_list = chunks_metadata if chunks_metadata else chunks
                if not chunks_list:
                    logger.error("No chunks provided to update_task_with_chunks")
                    return

                chunk_data = {}
                for i, chunk in enumerate(chunks_list):
                    chunk_id = chunk.get('chunk_id', f'chunk_{i:04d}')
                    chunk_data[chunk_id] = {
                        'status': 'PENDING',
                        'start_row': chunk.get('start_row', 0),
                        'end_row': chunk.get('end_row', 0),
                        'processed_records': 0,
                        'failed_records': 0,
                        'created_at': datetime.utcnow().isoformat()
                    }

                update_expr = 'SET chunks = :chunks, chunks_total = :total, #status = :status'
                expr_values = {
                    ':chunks': chunk_data,
                    ':total': total_chunks if total_chunks else len(chunks_list),
                    ':status': 'processing_chunks'
                }

                if estimated_total_records:
                    update_expr += ', total_records = :total_records'
                    expr_values[':total_records'] = estimated_total_records

                # Build update params
                update_params = {
                    'Key': {'task_id': task_id},
                    'UpdateExpression': update_expr,
                    'ExpressionAttributeValues': expr_values
                }
                
                # Only add ExpressionAttributeNames if needed
                if '#status' in update_expr:
                    update_params['ExpressionAttributeNames'] = {'#status': 'status'}
                
                self.table.update_item(**update_params)
                logger.info(f"Updated task {task_id} with {len(chunks_list)} chunks")
            except Exception as e:
                logger.error(f"Failed to update task with chunks: {e}")
                raise

        def update_chunk_status(self, task_id: str, chunk_id: str, status: str,
                              processed_records: int = None, failed_records: int = None,
                              vectors_upserted: int = None, error_message: str = None) -> None:
            """Update the status of a specific chunk and aggregate totals."""
            try:
                # First, get the current task to access chunks
                response = self.table.get_item(Key={'task_id': task_id})
                if 'Item' not in response:
                    logger.error(f"Task {task_id} not found")
                    return

                task = response['Item']
                chunks = task.get('chunks', {})

                # Update the specific chunk
                if chunk_id not in chunks:
                    chunks[chunk_id] = {}

                chunks[chunk_id]['status'] = status
                chunks[chunk_id]['updated_at'] = datetime.utcnow().isoformat()

                if processed_records is not None:
                    chunks[chunk_id]['processed_records'] = processed_records
                if failed_records is not None:
                    chunks[chunk_id]['failed_records'] = failed_records
                if vectors_upserted is not None:
                    chunks[chunk_id]['vectors_upserted'] = vectors_upserted
                if error_message:
                    chunks[chunk_id]['error_message'] = error_message

                # Calculate aggregate totals
                total_processed = sum(c.get('processed_records', 0) for c in chunks.values())
                total_failed = sum(c.get('failed_records', 0) for c in chunks.values())
                chunks_completed = sum(1 for c in chunks.values() if c.get('status') == 'COMPLETED')
                chunks_failed = sum(1 for c in chunks.values() if c.get('status') == 'FAILED')
                chunks_processing = sum(1 for c in chunks.values() if c.get('status') == 'IN_PROGRESS')

                # Update the task with chunk info and aggregated totals
                update_expr = '''SET
                    chunks = :chunks,
                    processed_records = :processed,
                    failed_records = :failed,
                    chunks_completed = :completed,
                    chunks_failed = :chunk_failed,
                    chunks_processing = :processing,
                    updated_at = :updated'''

                expr_values = {
                    ':chunks': chunks,
                    ':processed': total_processed,
                    ':failed': total_failed,
                    ':completed': chunks_completed,
                    ':chunk_failed': chunks_failed,
                    ':processing': chunks_processing,
                    ':updated': datetime.utcnow().isoformat()
                }

                # Update overall status based on chunk statuses
                if chunks_failed > 0 and chunks_completed + chunks_failed == len(chunks):
                    update_expr += ', #status = :status'
                    expr_values[':status'] = 'PARTIAL_SUCCESS'
                elif chunks_completed == len(chunks):
                    update_expr += ', #status = :status, completed_at = :completed_at'
                    expr_values[':status'] = 'COMPLETED'
                    expr_values[':completed_at'] = datetime.utcnow().isoformat()

                # Build update params
                update_params = {
                    'Key': {'task_id': task_id},
                    'UpdateExpression': update_expr,
                    'ExpressionAttributeValues': expr_values
                }
                
                # Only add ExpressionAttributeNames if needed
                if '#status' in update_expr:
                    update_params['ExpressionAttributeNames'] = {'#status': 'status'}
                
                self.table.update_item(**update_params)

                logger.info(f"Updated chunk {chunk_id} status to {status} for task {task_id}")
                logger.info(f"Aggregated totals - Processed: {total_processed}, Failed: {total_failed}, Chunks completed: {chunks_completed}/{len(chunks)}")

            except Exception as e:
                logger.error(f"Failed to update chunk status: {e}")
                # Don't raise to avoid breaking the processing flow
        
        def acquire_processing_lock(self, *args, **kwargs):
            """Stub method - always return True"""
            return True
        
        def release_processing_lock(self, *args, **kwargs):
            """Stub method - not needed for EC2 API"""
            pass
        
        def update_task_metadata(self, *args, **kwargs):
            """Stub method - not needed for EC2 API"""
            pass
        
        def get_migration_tasks(self, *args, **kwargs):
            """Stub method - return empty list"""
            return []