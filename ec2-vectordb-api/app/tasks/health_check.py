"""
Health check tasks for monitoring Celery workers and system health.
"""

import logging
import psutil
import redis
from datetime import datetime, timedelta
from typing import Dict, Any, List
from celery import current_task

from app.celery_config import celery_app
from app.config import settings
from app.services.dynamodb_client import DynamoDBClient
from app.services.openai_client import OpenAIClient
from app.services.pinecone_client import PineconeClient


logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    name='app.tasks.health_check.worker_health_check',
    max_retries=1
)
def worker_health_check(self) -> Dict[str, Any]:
    """
    Perform comprehensive health check of the Celery worker.
    
    Returns:
        Dict with health check results
    """
    
    health_status = {
        'timestamp': datetime.utcnow().isoformat(),
        'worker_id': self.request.hostname,
        'task_id': self.request.id,
        'overall_status': 'healthy',
        'checks': {}
    }
    
    try:
        # Check system resources
        health_status['checks']['system'] = check_system_resources()
        
        # Check Redis connectivity
        health_status['checks']['redis'] = check_redis_connectivity()
        
        # Check DynamoDB connectivity
        health_status['checks']['dynamodb'] = check_dynamodb_connectivity()
        
        # Check OpenAI API connectivity
        health_status['checks']['openai'] = check_openai_connectivity()
        
        # Check Pinecone connectivity
        health_status['checks']['pinecone'] = check_pinecone_connectivity()
        
        # Determine overall health status
        failed_checks = [
            check_name for check_name, check_result in health_status['checks'].items()
            if check_result.get('status') == 'unhealthy'
        ]
        
        if failed_checks:
            health_status['overall_status'] = 'unhealthy'
            health_status['failed_checks'] = failed_checks
        
        logger.info(f"Health check completed: {health_status['overall_status']}")
        
        if failed_checks:
            logger.warning(f"Failed health checks: {failed_checks}")
        
        return health_status
        
    except Exception as e:
        logger.error(f"Health check failed with error: {e}")
        health_status['overall_status'] = 'unhealthy'
        health_status['error'] = str(e)
        return health_status


@celery_app.task(
    bind=True,
    name='app.tasks.health_check.system_health_check',
    max_retries=1
)
def system_health_check(self) -> Dict[str, Any]:
    """
    Perform detailed system health check including external services.
    
    Returns:
        Dict with comprehensive system health information
    """
    
    system_health = {
        'timestamp': datetime.utcnow().isoformat(),
        'worker_id': self.request.hostname,
        'environment': settings.ENVIRONMENT,
        'checks': {},
        'summary': {}
    }
    
    try:
        # System resource checks
        system_health['checks']['cpu'] = check_cpu_usage()
        system_health['checks']['memory'] = check_memory_usage()
        system_health['checks']['disk'] = check_disk_usage()
        
        # External service checks
        system_health['checks']['redis'] = check_redis_connectivity()
        system_health['checks']['dynamodb'] = check_dynamodb_connectivity()
        system_health['checks']['openai'] = check_openai_api_status()
        system_health['checks']['pinecone'] = check_pinecone_api_status()
        
        # Celery queue health
        system_health['checks']['celery_queues'] = check_celery_queue_health()
        
        # Create summary
        healthy_checks = sum(1 for check in system_health['checks'].values() if check.get('status') == 'healthy')
        total_checks = len(system_health['checks'])
        
        system_health['summary'] = {
            'healthy_checks': healthy_checks,
            'total_checks': total_checks,
            'health_percentage': (healthy_checks / total_checks) * 100 if total_checks > 0 else 0,
            'overall_status': 'healthy' if healthy_checks == total_checks else 'degraded'
        }
        
        logger.info(f"System health check completed: {system_health['summary']['overall_status']}")
        return system_health
        
    except Exception as e:
        logger.error(f"System health check failed: {e}")
        system_health['summary'] = {
            'overall_status': 'unhealthy',
            'error': str(e)
        }
        return system_health


def check_system_resources() -> Dict[str, Any]:
    """Check system resource usage."""
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        resource_check = {
            'status': 'healthy',
            'cpu_usage_percent': cpu_percent,
            'memory_usage_percent': memory.percent,
            'disk_usage_percent': disk.percent,
            'available_memory_mb': memory.available // 1024 // 1024,
            'available_disk_gb': disk.free // 1024 // 1024 // 1024
        }
        
        # Mark as unhealthy if resources are critically low
        if cpu_percent > 90 or memory.percent > 90 or disk.percent > 95:
            resource_check['status'] = 'unhealthy'
            resource_check['warnings'] = []
            
            if cpu_percent > 90:
                resource_check['warnings'].append(f"High CPU usage: {cpu_percent}%")
            if memory.percent > 90:
                resource_check['warnings'].append(f"High memory usage: {memory.percent}%")
            if disk.percent > 95:
                resource_check['warnings'].append(f"High disk usage: {disk.percent}%")
        
        return resource_check
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': f"Failed to check system resources: {e}"
        }


def check_cpu_usage() -> Dict[str, Any]:
    """Check CPU usage details."""
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count()
        load_avg = psutil.getloadavg() if hasattr(psutil, 'getloadavg') else None
        
        return {
            'status': 'healthy' if cpu_percent < 80 else 'unhealthy',
            'cpu_percent': cpu_percent,
            'cpu_count': cpu_count,
            'load_average': load_avg
        }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': f"Failed to check CPU usage: {e}"
        }


def check_memory_usage() -> Dict[str, Any]:
    """Check memory usage details."""
    try:
        memory = psutil.virtual_memory()
        
        return {
            'status': 'healthy' if memory.percent < 85 else 'unhealthy',
            'total_mb': memory.total // 1024 // 1024,
            'available_mb': memory.available // 1024 // 1024,
            'used_mb': memory.used // 1024 // 1024,
            'percent': memory.percent
        }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': f"Failed to check memory usage: {e}"
        }


def check_disk_usage() -> Dict[str, Any]:
    """Check disk usage details."""
    try:
        disk = psutil.disk_usage('/')
        
        return {
            'status': 'healthy' if disk.percent < 90 else 'unhealthy',
            'total_gb': disk.total // 1024 // 1024 // 1024,
            'free_gb': disk.free // 1024 // 1024 // 1024,
            'used_gb': disk.used // 1024 // 1024 // 1024,
            'percent': disk.percent
        }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': f"Failed to check disk usage: {e}"
        }


def check_redis_connectivity() -> Dict[str, Any]:
    """Check Redis server connectivity."""
    try:
        redis_client = redis.from_url(settings.redis_url)
        
        # Test basic connectivity
        response = redis_client.ping()
        if not response:
            return {
                'status': 'unhealthy',
                'error': 'Redis ping failed'
            }
        
        # Get Redis info
        info = redis_client.info()
        
        return {
            'status': 'healthy',
            'version': info.get('redis_version'),
            'connected_clients': info.get('connected_clients'),
            'used_memory_mb': info.get('used_memory', 0) // 1024 // 1024,
            'uptime_seconds': info.get('uptime_in_seconds')
        }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': f"Redis connectivity check failed: {e}"
        }


def check_dynamodb_connectivity() -> Dict[str, Any]:
    """Check DynamoDB connectivity."""
    try:
        dynamodb = DynamoDBClient()
        
        # Try to describe the table (doesn't require data)
        table_desc = dynamodb.table.meta.client.describe_table(
            TableName=dynamodb.table_name
        )
        
        return {
            'status': 'healthy',
            'table_name': dynamodb.table_name,
            'table_status': table_desc['Table']['TableStatus'],
            'item_count': table_desc['Table'].get('ItemCount', 'N/A')
        }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': f"DynamoDB connectivity check failed: {e}"
        }


def check_openai_connectivity() -> Dict[str, Any]:
    """Check OpenAI API connectivity with minimal test."""
    try:
        openai_client = OpenAIClient()
        
        # Test with a very short text to minimize cost
        test_embedding = openai_client.get_embedding("test")
        
        if test_embedding and len(test_embedding) > 0:
            return {
                'status': 'healthy',
                'model': settings.OPENAI_MODEL,
                'embedding_dimension': len(test_embedding)
            }
        else:
            return {
                'status': 'unhealthy',
                'error': 'Failed to generate test embedding'
            }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': f"OpenAI connectivity check failed: {e}"
        }


def check_pinecone_connectivity() -> Dict[str, Any]:
    """Check Pinecone connectivity."""
    try:
        pinecone_client = PineconeClient()
        
        # Get index statistics
        stats = pinecone_client.get_index_stats()
        
        return {
            'status': 'healthy',
            'index_name': settings.PINECONE_INDEX_NAME,
            'vector_count': stats.get('total_vector_count', 0),
            'index_fullness': stats.get('index_fullness', 0),
            'dimension': stats.get('dimension', 'N/A')
        }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': f"Pinecone connectivity check failed: {e}"
        }


def check_openai_api_status() -> Dict[str, Any]:
    """Check OpenAI API status without making embedding calls."""
    try:
        # This is a basic connectivity check that doesn't use API quota
        import openai
        
        # Set API key
        openai.api_key = settings.OPENAI_API_KEY
        
        # Just validate the API key format and settings
        if not settings.OPENAI_API_KEY or not settings.OPENAI_API_KEY.startswith('sk-'):
            return {
                'status': 'unhealthy',
                'error': 'Invalid OpenAI API key format'
            }
        
        return {
            'status': 'healthy',
            'model': settings.OPENAI_MODEL,
            'rate_limit': settings.OPENAI_RATE_LIMIT
        }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': f"OpenAI API status check failed: {e}"
        }


def check_pinecone_api_status() -> Dict[str, Any]:
    """Check Pinecone API status."""
    try:
        pinecone_client = PineconeClient()
        
        # Try to get index description
        index_info = pinecone_client.describe_index()
        
        return {
            'status': 'healthy',
            'index_name': settings.PINECONE_INDEX_NAME,
            'status': index_info.get('status', {}).get('ready', False),
            'host': index_info.get('status', {}).get('host', 'N/A')
        }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': f"Pinecone API status check failed: {e}"
        }


def check_celery_queue_health() -> Dict[str, Any]:
    """Check Celery queue health and statistics."""
    try:
        from celery import current_app
        
        inspect = current_app.control.inspect()
        
        # Get active tasks
        active_tasks = inspect.active()
        
        # Get queue lengths (this requires Redis inspection)
        redis_client = redis.from_url(settings.redis_url)
        
        queue_lengths = {}
        queue_names = ['data_processing', 'chunk_processing', 'monitoring']
        
        for queue_name in queue_names:
            queue_key = f"celery:{queue_name}"
            length = redis_client.llen(queue_key)
            queue_lengths[queue_name] = length
        
        total_active = sum(len(tasks) for tasks in (active_tasks or {}).values())
        total_queued = sum(queue_lengths.values())
        
        return {
            'status': 'healthy',
            'active_tasks': total_active,
            'queued_tasks': total_queued,
            'queue_lengths': queue_lengths,
            'active_workers': len(active_tasks or {})
        }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': f"Celery queue health check failed: {e}"
        }