"""
Progress tracking utilities for background tasks.
Provides real-time progress updates and monitoring capabilities.
"""

import logging
import redis
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from enum import Enum

from app.config import settings
from app.services.dynamodb_client import DynamoDBClient


logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task status enumeration."""
    QUEUED = "QUEUED"
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    PROCESSING_CHUNKS = "PROCESSING_CHUNKS"
    INITIALIZING = "INITIALIZING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    PARTIALLY_COMPLETED = "PARTIALLY_COMPLETED"


@dataclass
class ProgressUpdate:
    """Data class for progress updates."""
    task_id: str
    current: int
    total: int
    status: str
    message: str = ""
    progress_percentage: float = 0.0
    estimated_completion_time: Optional[str] = None
    metadata: Dict[str, Any] = None
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat()
        
        if self.total > 0:
            self.progress_percentage = (self.current / self.total) * 100
        
        if self.metadata is None:
            self.metadata = {}


class ProgressTracker:
    """
    Real-time progress tracking for background tasks.
    Uses Redis for fast updates and DynamoDB for persistent storage.
    """
    
    def __init__(self):
        self.redis_client = redis.from_url(settings.redis_url)
        self.dynamodb = DynamoDBClient()
        self.progress_key_prefix = "progress:"
        self.task_subscribers_key_prefix = "subscribers:"
        self.progress_ttl = 3600  # 1 hour TTL for Redis progress data
        
    def update_progress(self, task_id: str, current: int, total: int = None, 
                       status: str = None, message: str = "", 
                       metadata: Dict[str, Any] = None, 
                       persist_to_db: bool = True) -> None:
        """
        Update task progress with real-time notifications.
        
        Args:
            task_id: Task identifier
            current: Current number of processed items
            total: Total number of items to process
            status: Current task status
            message: Progress message
            metadata: Additional metadata
            persist_to_db: Whether to persist to DynamoDB
        """
        try:
            # Get existing progress to preserve total if not provided
            if total is None:
                existing_progress = self.get_progress(task_id)
                if existing_progress:
                    total = existing_progress.get('total', current)
                else:
                    total = current
            
            # Create progress update
            progress_update = ProgressUpdate(
                task_id=task_id,
                current=current,
                total=total,
                status=status or TaskStatus.IN_PROGRESS.value,
                message=message,
                metadata=metadata or {}
            )
            
            # Calculate estimated completion time
            if current > 0 and total > current:
                progress_update.estimated_completion_time = self._calculate_eta(
                    task_id, current, total
                )
            
            # Store in Redis for real-time access
            progress_key = f"{self.progress_key_prefix}{task_id}"
            progress_data = asdict(progress_update)
            
            self.redis_client.setex(
                progress_key, 
                self.progress_ttl, 
                json.dumps(progress_data, default=str)
            )
            
            # Publish progress update to subscribers
            self._publish_progress_update(task_id, progress_update)
            
            # Persist to DynamoDB if requested
            if persist_to_db:
                self.dynamodb.update_task_progress(
                    task_id=task_id,
                    processed_records=current,
                    total_records=total,
                    status=status,
                    metadata={
                        'last_message': message,
                        'estimated_completion_time': progress_update.estimated_completion_time,
                        **(metadata or {})
                    }
                )
            
            logger.debug(f"Updated progress for task {task_id}: {current}/{total} ({progress_update.progress_percentage:.1f}%)")
            
        except Exception as e:
            logger.error(f"Error updating progress for task {task_id}: {e}")
            # Don't raise - progress updates shouldn't break the main task
    
    def get_progress(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current progress for a task.
        
        Args:
            task_id: Task identifier
            
        Returns:
            Progress data or None if not found
        """
        try:
            # Try Redis first for real-time data
            progress_key = f"{self.progress_key_prefix}{task_id}"
            progress_data = self.redis_client.get(progress_key)
            
            if progress_data:
                return json.loads(progress_data)
            
            # Fallback to DynamoDB
            task = self.dynamodb.get_task(task_id)
            if task:
                return {
                    'task_id': task_id,
                    'current': task.get('processed_records', 0),
                    'total': task.get('total_records', 0),
                    'status': task.get('status', TaskStatus.PENDING.value),
                    'progress_percentage': task.get('progress_percentage', 0),
                    'message': task.get('last_message', ''),
                    'estimated_completion_time': task.get('estimated_completion_time'),
                    'timestamp': task.get('updated_at'),
                    'metadata': {
                        'created_at': task.get('created_at'),
                        'task_type': task.get('task_type'),
                        'failed_records': task.get('failed_records', 0)
                    }
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting progress for task {task_id}: {e}")
            return None
    
    def get_multiple_progress(self, task_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get progress for multiple tasks efficiently.
        
        Args:
            task_ids: List of task identifiers
            
        Returns:
            Dict mapping task_id to progress data
        """
        progress_data = {}
        
        try:
            # Batch get from Redis
            redis_keys = [f"{self.progress_key_prefix}{tid}" for tid in task_ids]
            redis_values = self.redis_client.mget(redis_keys)
            
            for task_id, value in zip(task_ids, redis_values):
                if value:
                    progress_data[task_id] = json.loads(value)
            
            # For missing tasks, get from DynamoDB
            missing_task_ids = [tid for tid in task_ids if tid not in progress_data]
            
            if missing_task_ids:
                for task_id in missing_task_ids:
                    progress = self.get_progress(task_id)
                    if progress:
                        progress_data[task_id] = progress
            
            return progress_data
            
        except Exception as e:
            logger.error(f"Error getting multiple progress updates: {e}")
            return {}
    
    def subscribe_to_progress(self, task_id: str, subscriber_id: str) -> None:
        """
        Subscribe to progress updates for a task.
        
        Args:
            task_id: Task identifier
            subscriber_id: Subscriber identifier (e.g., user ID, session ID)
        """
        try:
            subscribers_key = f"{self.task_subscribers_key_prefix}{task_id}"
            self.redis_client.sadd(subscribers_key, subscriber_id)
            self.redis_client.expire(subscribers_key, self.progress_ttl)
            
            logger.debug(f"Subscribed {subscriber_id} to task {task_id} progress")
            
        except Exception as e:
            logger.error(f"Error subscribing to progress for task {task_id}: {e}")
    
    def unsubscribe_from_progress(self, task_id: str, subscriber_id: str) -> None:
        """
        Unsubscribe from progress updates for a task.
        
        Args:
            task_id: Task identifier
            subscriber_id: Subscriber identifier
        """
        try:
            subscribers_key = f"{self.task_subscribers_key_prefix}{task_id}"
            self.redis_client.srem(subscribers_key, subscriber_id)
            
            logger.debug(f"Unsubscribed {subscriber_id} from task {task_id} progress")
            
        except Exception as e:
            logger.error(f"Error unsubscribing from progress for task {task_id}: {e}")
    
    def get_task_subscribers(self, task_id: str) -> List[str]:
        """
        Get list of subscribers for a task.
        
        Args:
            task_id: Task identifier
            
        Returns:
            List of subscriber IDs
        """
        try:
            subscribers_key = f"{self.task_subscribers_key_prefix}{task_id}"
            subscribers = self.redis_client.smembers(subscribers_key)
            return [s.decode('utf-8') if isinstance(s, bytes) else s for s in subscribers]
            
        except Exception as e:
            logger.error(f"Error getting subscribers for task {task_id}: {e}")
            return []
    
    def cleanup_completed_progress(self, task_id: str) -> None:
        """
        Clean up progress data for completed tasks.
        
        Args:
            task_id: Task identifier
        """
        try:
            progress_key = f"{self.progress_key_prefix}{task_id}"
            subscribers_key = f"{self.task_subscribers_key_prefix}{task_id}"
            
            self.redis_client.delete(progress_key)
            self.redis_client.delete(subscribers_key)
            
            logger.debug(f"Cleaned up progress data for task {task_id}")
            
        except Exception as e:
            logger.error(f"Error cleaning up progress data for task {task_id}: {e}")
    
    def get_active_tasks_progress(self) -> Dict[str, Dict[str, Any]]:
        """
        Get progress for all active tasks.
        
        Returns:
            Dict mapping task_id to progress data
        """
        try:
            # Get active tasks from DynamoDB
            active_tasks = self.dynamodb.get_active_tasks()
            task_ids = [task['task_id'] for task in active_tasks]
            
            return self.get_multiple_progress(task_ids)
            
        except Exception as e:
            logger.error(f"Error getting active tasks progress: {e}")
            return {}
    
    def _calculate_eta(self, task_id: str, current: int, total: int) -> Optional[str]:
        """
        Calculate estimated time of completion based on processing rate.
        
        Args:
            task_id: Task identifier
            current: Current progress
            total: Total items
            
        Returns:
            ISO formatted estimated completion time
        """
        try:
            # Get task start time
            task = self.dynamodb.get_task(task_id)
            if not task or not task.get('started_at'):
                return None
            
            started_at = datetime.fromisoformat(task['started_at'].replace('Z', '+00:00'))
            now = datetime.utcnow()
            
            elapsed_time = now - started_at
            elapsed_seconds = elapsed_time.total_seconds()
            
            if elapsed_seconds <= 0 or current <= 0:
                return None
            
            # Calculate processing rate (items per second)
            rate = current / elapsed_seconds
            
            # Calculate remaining time
            remaining_items = total - current
            if remaining_items <= 0:
                return now.isoformat()
            
            remaining_seconds = remaining_items / rate
            eta = now + timedelta(seconds=remaining_seconds)
            
            return eta.isoformat()
            
        except Exception as e:
            logger.debug(f"Error calculating ETA for task {task_id}: {e}")
            return None
    
    def _publish_progress_update(self, task_id: str, progress_update: ProgressUpdate) -> None:
        """
        Publish progress update to subscribed clients.
        
        Args:
            task_id: Task identifier
            progress_update: Progress update data
        """
        try:
            # Get subscribers
            subscribers = self.get_task_subscribers(task_id)
            
            if not subscribers:
                return
            
            # Publish to Redis pub/sub channel
            channel = f"task_progress:{task_id}"
            message = json.dumps(asdict(progress_update), default=str)
            
            self.redis_client.publish(channel, message)
            
            logger.debug(f"Published progress update for task {task_id} to {len(subscribers)} subscribers")
            
        except Exception as e:
            logger.error(f"Error publishing progress update for task {task_id}: {e}")
    
    def listen_to_progress_updates(self, task_ids: List[str]):
        """
        Generator that yields progress updates for specified tasks.
        
        Args:
            task_ids: List of task identifiers to monitor
            
        Yields:
            Dict with task progress updates
        """
        try:
            pubsub = self.redis_client.pubsub()
            
            # Subscribe to channels
            channels = [f"task_progress:{task_id}" for task_id in task_ids]
            for channel in channels:
                pubsub.subscribe(channel)
            
            logger.info(f"Listening for progress updates on {len(channels)} channels")
            
            for message in pubsub.listen():
                if message['type'] == 'message':
                    try:
                        channel = message['channel'].decode('utf-8')
                        task_id = channel.split(':')[1]  # Extract task_id from channel name
                        data = json.loads(message['data'])
                        
                        yield {
                            'task_id': task_id,
                            'channel': channel,
                            'progress': data
                        }
                    except Exception as e:
                        logger.error(f"Error processing progress message: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error in progress updates listener: {e}")
        finally:
            try:
                pubsub.close()
            except:
                pass


# Global progress tracker instance
progress_tracker = ProgressTracker()