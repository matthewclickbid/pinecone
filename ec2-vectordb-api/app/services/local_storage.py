"""
Local storage implementation for development/testing without AWS dependencies.
Stores task data in a local JSON file instead of DynamoDB.
"""

import json
import os
import uuid
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from threading import Lock
from pathlib import Path

logger = logging.getLogger(__name__)


class LocalStorageClient:
    """
    Local file-based storage client that mimics DynamoDBClient interface.
    Used for local testing without AWS dependencies.
    """
    
    def __init__(self):
        self.storage_dir = Path("./local_storage")
        self.storage_dir.mkdir(exist_ok=True)
        self.tasks_file = self.storage_dir / "tasks.json"
        self.lock = Lock()
        
        # Initialize storage file if it doesn't exist
        if not self.tasks_file.exists():
            self._save_data({})
        
        logger.info(f"Initialized local storage at {self.tasks_file}")
    
    def _load_data(self) -> Dict[str, Any]:
        """Load data from JSON file."""
        try:
            with open(self.tasks_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
    
    def _save_data(self, data: Dict[str, Any]) -> None:
        """Save data to JSON file."""
        with open(self.tasks_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def create_task(self, start_date: str, end_date: str, total_records: int = 0, task_type: str = 'data_processing') -> str:
        """Create a new task in local storage."""
        with self.lock:
            task_id = str(uuid.uuid4())
            timestamp = datetime.utcnow().isoformat()
            
            task = {
                'task_id': task_id,
                'status': 'pending',
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
            
            data = self._load_data()
            data[task_id] = task
            self._save_data(data)
            
            logger.info(f"Created task {task_id} with status PENDING")
            return task_id
    
    def update_task_status(self, task_id: str, status: str, **kwargs) -> None:
        """Update task status and other fields."""
        with self.lock:
            data = self._load_data()
            
            if task_id not in data:
                logger.warning(f"Task {task_id} not found")
                return
            
            # Map status values to API-compatible ones
            status_map = {
                'PENDING': 'pending',
                'QUEUED': 'queued', 
                'IN_PROGRESS': 'processing',
                'PROCESSING': 'processing',
                'COMPLETED': 'completed',
                'FAILED': 'failed',
                'CANCELLED': 'cancelled'
            }
            normalized_status = status_map.get(status.upper(), status.lower())
            
            data[task_id]['status'] = normalized_status
            data[task_id]['updated_at'] = datetime.utcnow().isoformat()
            
            # Update additional fields
            for key, value in kwargs.items():
                if key not in ['task_id']:
                    data[task_id][key] = value
            
            self._save_data(data)
            logger.info(f"Updated task {task_id} status to {status}")
    
    def increment_processed_records(self, task_id: str, increment: int = 1) -> None:
        """Increment the processed records count for a task."""
        with self.lock:
            data = self._load_data()
            
            if task_id not in data:
                logger.warning(f"Task {task_id} not found")
                return
            
            data[task_id]['processed_records'] = data[task_id].get('processed_records', 0) + increment
            data[task_id]['updated_at'] = datetime.utcnow().isoformat()
            
            self._save_data(data)
            logger.debug(f"Incremented processed records for task {task_id} by {increment}")
    
    def increment_failed_records(self, task_id: str, increment: int = 1) -> None:
        """Increment the failed records count for a task."""
        with self.lock:
            data = self._load_data()
            
            if task_id not in data:
                logger.warning(f"Task {task_id} not found")
                return
            
            data[task_id]['failed_records'] = data[task_id].get('failed_records', 0) + increment
            data[task_id]['updated_at'] = datetime.utcnow().isoformat()
            
            self._save_data(data)
            logger.debug(f"Incremented failed records for task {task_id} by {increment}")
    
    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get task details by task ID."""
        with self.lock:
            data = self._load_data()
            return data.get(task_id)
    
    def set_task_error(self, task_id: str, error_message: str) -> None:
        """Set task status to FAILED with error message."""
        self.update_task_status(task_id, 'FAILED', error_message=error_message)
        logger.error(f"Task {task_id} failed: {error_message}")
    
    def create_background_task(self, task_type: str, parameters: Dict[str, Any], 
                             priority: str = 'normal', celery_task_id: str = None) -> str:
        """Create a background task for Celery processing."""
        with self.lock:
            task_id = str(uuid.uuid4())
            timestamp = datetime.utcnow().isoformat()
            
            task = {
                'task_id': task_id,
                'status': 'queued',
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
            
            data = self._load_data()
            data[task_id] = task
            self._save_data(data)
            
            logger.info(f"Created background task {task_id} with type {task_type}")
            return task_id
    
    def update_task_progress(self, task_id: str, processed_records: int, 
                           total_records: int = None, status: str = None, 
                           metadata: Dict[str, Any] = None) -> None:
        """Update task progress with detailed tracking."""
        with self.lock:
            data = self._load_data()
            
            if task_id not in data:
                logger.warning(f"Task {task_id} not found")
                return
            
            task = data[task_id]
            current_time = datetime.utcnow().isoformat()
            
            task['updated_at'] = current_time
            task['processed_records'] = processed_records
            
            if total_records is not None:
                task['total_records'] = total_records
                # Calculate progress percentage
                if total_records > 0:
                    task['progress_percentage'] = (processed_records / total_records) * 100
            
            if status:
                task['status'] = status
                # Track status change timestamps
                if status == 'IN_PROGRESS':
                    task['started_at'] = current_time
                elif status in ['COMPLETED', 'FAILED', 'CANCELLED']:
                    task['completed_at'] = current_time
            
            if metadata:
                for key, value in metadata.items():
                    if key not in ['task_id']:
                        task[key] = value
            
            self._save_data(data)
            logger.debug(f"Updated task {task_id} progress: {processed_records} processed")
    
    def get_tasks_by_status(self, status: str, task_type: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get tasks by status, optionally filtered by type."""
        with self.lock:
            data = self._load_data()
            tasks = []
            
            for task in data.values():
                if task.get('status') == status:
                    if task_type is None or task.get('task_type') == task_type:
                        tasks.append(task)
                        if len(tasks) >= limit:
                            break
            
            return tasks
    
    def get_active_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all active (non-completed) tasks."""
        active_statuses = ['QUEUED', 'PENDING', 'IN_PROGRESS', 'PROCESSING_CHUNKS', 'INITIALIZING']
        
        with self.lock:
            data = self._load_data()
            tasks = []
            
            for task in data.values():
                if task.get('status') in active_statuses:
                    tasks.append(task)
                    if len(tasks) >= limit:
                        break
            
            return tasks
    
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
        with self.lock:
            data = self._load_data()
            
            # Calculate time range
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=time_period_hours)
            
            stats = {
                'time_period_hours': time_period_hours,
                'total_tasks': 0,
                'status_counts': {},
                'type_counts': {},
                'total_records_processed': 0,
                'total_records_failed': 0,
                'average_processing_time_minutes': 0,
                'success_rate_percentage': 0
            }
            
            processing_times = []
            
            for task in data.values():
                try:
                    updated_at = datetime.fromisoformat(task.get('updated_at', '').replace('Z', '+00:00'))
                    if updated_at < start_time:
                        continue
                except:
                    continue
                
                stats['total_tasks'] += 1
                
                # Status counts
                status = task.get('status', 'unknown')
                stats['status_counts'][status] = stats['status_counts'].get(status, 0) + 1
                
                # Type counts
                task_type = task.get('task_type', 'unknown')
                stats['type_counts'][task_type] = stats['type_counts'].get(task_type, 0) + 1
                
                # Record counts
                stats['total_records_processed'] += task.get('processed_records', 0)
                stats['total_records_failed'] += task.get('failed_records', 0)
                
                # Processing time
                if task.get('started_at') and task.get('completed_at'):
                    try:
                        started = datetime.fromisoformat(task['started_at'].replace('Z', '+00:00'))
                        completed = datetime.fromisoformat(task['completed_at'].replace('Z', '+00:00'))
                        duration = (completed - started).total_seconds() / 60  # minutes
                        processing_times.append(duration)
                    except:
                        pass
            
            # Calculate averages
            if processing_times:
                stats['average_processing_time_minutes'] = sum(processing_times) / len(processing_times)
            
            total_records = stats['total_records_processed'] + stats['total_records_failed']
            if total_records > 0:
                stats['success_rate_percentage'] = (stats['total_records_processed'] / total_records) * 100
            
            return stats
    
    def cleanup_completed_tasks(self, retention_days: int = 7) -> int:
        """Clean up completed tasks older than retention period."""
        with self.lock:
            data = self._load_data()
            cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
            
            tasks_to_delete = []
            for task_id, task in data.items():
                if task.get('status') in ['COMPLETED', 'FAILED', 'CANCELLED']:
                    try:
                        updated_at = datetime.fromisoformat(task.get('updated_at', '').replace('Z', '+00:00'))
                        if updated_at < cutoff_date:
                            tasks_to_delete.append(task_id)
                    except:
                        pass
            
            for task_id in tasks_to_delete:
                del data[task_id]
            
            self._save_data(data)
            
            logger.info(f"Cleaned up {len(tasks_to_delete)} old task records")
            return len(tasks_to_delete)
    
    # Stub methods for DynamoDB-specific features not needed in local mode
    def update_task_with_chunks(self, *args, **kwargs):
        """Stub method - not needed for local testing"""
        pass
    
    def update_chunk_status(self, task_id: str, chunk_id: str, status: str, 
                           error_message: str = None, processed_records: int = None,
                           failed_records: int = None, vectors_upserted: int = None):
        """Update chunk status and increment task totals atomically."""
        with self.lock:
            data = self._load_data()
            
            if task_id not in data:
                logger.warning(f"Task {task_id} not found for chunk update")
                return
            
            task = data[task_id]
            current_time = datetime.utcnow().isoformat()
            task['updated_at'] = current_time
            
            # Store chunk status in task metadata
            if 'chunks' not in task:
                task['chunks'] = {}
            
            task['chunks'][chunk_id] = {
                'status': status,
                'updated_at': current_time,
                'error_message': error_message
            }
            
            # Increment task totals atomically (don't overwrite)
            if processed_records is not None and processed_records > 0:
                # Only increment if this chunk hasn't been counted before
                if chunk_id not in task.get('chunk_processed', {}):
                    task['processed_records'] = task.get('processed_records', 0) + processed_records
                    if 'chunk_processed' not in task:
                        task['chunk_processed'] = {}
                    task['chunk_processed'][chunk_id] = processed_records
            
            if failed_records is not None and failed_records > 0:
                if chunk_id not in task.get('chunk_failed', {}):
                    task['failed_records'] = task.get('failed_records', 0) + failed_records
                    if 'chunk_failed' not in task:
                        task['chunk_failed'] = {}
                    task['chunk_failed'][chunk_id] = failed_records
            
            if vectors_upserted is not None and vectors_upserted > 0:
                if chunk_id not in task.get('chunk_vectors', {}):
                    task['vectors_upserted'] = task.get('vectors_upserted', 0) + vectors_upserted
                    if 'chunk_vectors' not in task:
                        task['chunk_vectors'] = {}
                    task['chunk_vectors'][chunk_id] = vectors_upserted
            
            # Update chunk completion count
            if status == 'COMPLETED':
                task['chunks_completed'] = task.get('chunks_completed', 0) + 1
            elif status == 'FAILED':
                task['chunks_failed'] = task.get('chunks_failed', 0) + 1
            
            self._save_data(data)
            logger.debug(f"Updated chunk {chunk_id} status to {status} for task {task_id}")
    
    def acquire_processing_lock(self, *args, **kwargs):
        """Stub method - always return True in local mode"""
        return True
    
    def release_processing_lock(self, *args, **kwargs):
        """Stub method - not needed for local testing"""
        pass
    
    def update_task_metadata(self, *args, **kwargs):
        """Stub method - not needed for local testing"""
        pass
    
    def get_migration_tasks(self, *args, **kwargs):
        """Stub method - return empty list in local mode"""
        return []