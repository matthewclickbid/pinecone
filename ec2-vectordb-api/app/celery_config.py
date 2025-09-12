"""
Celery configuration for background task processing.
Integrates with Redis broker and provides task routing and configuration.
"""

import os
from datetime import timedelta
from celery import Celery
from kombu import Queue
from app.config import settings


def make_celery() -> Celery:
    """
    Create and configure Celery application instance.
    
    Returns:
        Celery: Configured Celery application
    """
    
    # Create Celery instance
    celery = Celery(
        'vectordb_processor',
        broker=settings.redis_url,
        backend=settings.redis_url,
        include=[
            'app.tasks.data_processing',
            'app.tasks.health_check',
            'app.tasks.monitoring'
        ]
    )
    
    # Update configuration
    celery.conf.update(
        # Task routing and queues
        task_routes={
            'app.tasks.data_processing.process_metabase_data': {'queue': 'data_processing'},
            'app.tasks.data_processing.process_s3_csv_data': {'queue': 'data_processing'},
            'app.tasks.data_processing.process_csv_chunk': {'queue': 'chunk_processing'},
            'app.tasks.data_processing.aggregate_chunk_results': {'queue': 'data_processing'},
            'app.tasks.health_check.*': {'queue': 'monitoring'},
            'app.tasks.monitoring.*': {'queue': 'monitoring'}
        },
        
        # Define queues with priority
        task_queues=(
            Queue('data_processing', routing_key='data_processing', queue_arguments={'x-max-priority': 5}),
            Queue('chunk_processing', routing_key='chunk_processing', queue_arguments={'x-max-priority': 3}),
            Queue('monitoring', routing_key='monitoring', queue_arguments={'x-max-priority': 1}),
        ),
        
        # Task execution settings
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
        
        # Result backend settings
        result_backend=settings.redis_url,
        result_expires=86400,  # 24 hours
        result_persistent=True,
        
        # Worker settings
        worker_prefetch_multiplier=1,  # Prevent worker from hoarding tasks
        task_acks_late=True,  # Acknowledge tasks only after completion
        worker_max_tasks_per_child=1000,  # Restart worker after 1000 tasks to prevent memory leaks
        
        # Task time limits
        task_soft_time_limit=settings.TASK_TIMEOUT - 60,  # Soft limit 1 minute before hard limit
        task_time_limit=settings.TASK_TIMEOUT,
        
        # Retry settings
        task_default_retry_delay=60,  # Default retry delay: 1 minute
        task_max_retries=3,
        
        # Monitoring and logging
        worker_send_task_events=True,
        task_send_sent_event=True,
        
        # Security settings
        worker_hijack_root_logger=False,
        worker_log_color=settings.is_development,
        
        # Beat schedule for periodic tasks (if needed)
        beat_schedule={
            'health-check': {
                'task': 'app.tasks.health_check.worker_health_check',
                'schedule': timedelta(minutes=5),
                'options': {'queue': 'monitoring'}
            },
            'cleanup-old-tasks': {
                'task': 'app.tasks.monitoring.cleanup_old_task_records',
                'schedule': timedelta(hours=24),
                'options': {'queue': 'monitoring'}
            }
        },
        
        # Additional settings for production
        broker_connection_retry_on_startup=True,
        broker_connection_retry=True,
        broker_connection_max_retries=10,
        
        # Task compression
        task_compression='gzip',
        result_compression='gzip',
        
        # Memory optimization
        worker_disable_rate_limits=True,
        task_ignore_result=False,  # We want to track results
        
        # Error handling
        task_reject_on_worker_lost=True,
        task_create_missing_queues=True,
    )
    
    return celery


# Create the Celery instance
celery_app = make_celery()


class CeleryConfig:
    """
    Celery configuration class for additional customization.
    """
    
    @staticmethod
    def configure_logging():
        """Configure logging for Celery workers."""
        import logging
        import sys
        
        # Configure root logger
        logging.basicConfig(
            level=getattr(logging, settings.LOG_LEVEL.upper()),
            format=settings.LOG_FORMAT,
            stream=sys.stdout
        )
        
        # Suppress noisy loggers in production
        if settings.is_production:
            logging.getLogger('urllib3').setLevel(logging.WARNING)
            logging.getLogger('botocore').setLevel(logging.WARNING)
            logging.getLogger('boto3').setLevel(logging.WARNING)
    
    @staticmethod
    def get_worker_concurrency() -> int:
        """
        Get optimal worker concurrency based on environment.
        
        Returns:
            int: Number of concurrent workers
        """
        import multiprocessing
        
        if settings.is_production:
            # In production, use more conservative settings
            return min(settings.MAX_WORKERS, multiprocessing.cpu_count())
        else:
            # In development, use fewer workers to avoid resource contention
            return min(4, multiprocessing.cpu_count())
    
    @staticmethod
    def get_queue_configuration():
        """
        Get queue configuration for different task types.
        
        Returns:
            dict: Queue configuration mapping
        """
        return {
            'data_processing': {
                'concurrency': CeleryConfig.get_worker_concurrency(),
                'max_memory_per_child': 200000,  # 200MB per child process
                'prefetch_multiplier': 1
            },
            'chunk_processing': {
                'concurrency': min(8, CeleryConfig.get_worker_concurrency() * 2),
                'max_memory_per_child': 100000,  # 100MB per child process  
                'prefetch_multiplier': 2
            },
            'monitoring': {
                'concurrency': 2,
                'max_memory_per_child': 50000,  # 50MB per child process
                'prefetch_multiplier': 4
            }
        }


# Configure logging when module is imported
CeleryConfig.configure_logging()


# Celery signal handlers for monitoring and cleanup
@celery_app.task(bind=True)
def debug_task(self):
    """Debug task for testing Celery configuration."""
    print(f'Request: {self.request!r}')
    return {'status': 'success', 'message': 'Debug task completed'}


# Task failure handler
@celery_app.task(bind=True, base=celery_app.Task)
def handle_task_failure(self, exc, task_id, args, kwargs, traceback):
    """Handle task failures with proper logging and cleanup."""
    import logging
    logger = logging.getLogger(__name__)
    
    logger.error(f"Task {task_id} failed with exception: {exc}")
    logger.error(f"Traceback: {traceback}")
    
    # Additional cleanup logic can be added here
    return {'status': 'failed', 'exception': str(exc), 'task_id': task_id}