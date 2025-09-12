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
            current_time = datetime.utcnow().isoformat()
            
            update_data = {
                'status': 'CANCELLED',
                'updated_at': current_time,
                'completed_at': current_time
            }
            
            if reason:
                update_data['cancellation_reason'] = reason
            
            self.update_task_status(task_id, 'CANCELLED', **update_data)
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
        
        # Stub methods for Step Functions features (not needed for EC2 API)
        def update_task_with_chunks(self, *args, **kwargs):
            """Stub method - not needed for EC2 API"""
            pass
        
        def update_chunk_status(self, *args, **kwargs):
            """Stub method - not needed for EC2 API"""
            pass
        
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