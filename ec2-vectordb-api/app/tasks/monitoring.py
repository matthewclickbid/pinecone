"""
Monitoring and maintenance tasks for the VectorDB processing system.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from collections import defaultdict

from app.celery_config import celery_app
from app.config import settings
from app.services.dynamodb_client import DynamoDBClient


logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    name='app.tasks.monitoring.cleanup_old_task_records',
    max_retries=2
)
def cleanup_old_task_records(self, retention_days: int = 30) -> Dict[str, Any]:
    """
    Clean up old task records from DynamoDB.
    
    Args:
        retention_days: Number of days to retain completed tasks
        
    Returns:
        Dict with cleanup results
    """
    
    dynamodb = DynamoDBClient()
    
    try:
        logger.info(f"Starting cleanup of task records older than {retention_days} days")
        
        # Calculate cutoff date
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
        cutoff_timestamp = cutoff_date.isoformat()
        
        # Scan for old completed/failed tasks
        # Note: In production, you might want to use a GSI for better performance
        response = dynamodb.table.scan(
            FilterExpression='#status IN (:completed, :failed) AND updated_at < :cutoff',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':completed': 'COMPLETED',
                ':failed': 'FAILED',
                ':cutoff': cutoff_timestamp
            }
        )
        
        old_tasks = response.get('Items', [])
        deleted_count = 0
        failed_deletions = 0
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = dynamodb.table.scan(
                FilterExpression='#status IN (:completed, :failed) AND updated_at < :cutoff',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={
                    ':completed': 'COMPLETED',
                    ':failed': 'FAILED',
                    ':cutoff': cutoff_timestamp
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            old_tasks.extend(response.get('Items', []))
        
        logger.info(f"Found {len(old_tasks)} old task records to clean up")
        
        # Delete old tasks in batches
        batch_size = 25  # DynamoDB batch write limit
        
        for i in range(0, len(old_tasks), batch_size):
            batch = old_tasks[i:i + batch_size]
            
            try:
                with dynamodb.table.batch_writer() as batch_writer:
                    for task in batch:
                        batch_writer.delete_item(Key={'task_id': task['task_id']})
                        deleted_count += 1
                        
                logger.debug(f"Deleted batch of {len(batch)} task records")
                
            except Exception as e:
                logger.error(f"Failed to delete batch: {e}")
                failed_deletions += len(batch)
        
        result = {
            'status': 'completed',
            'retention_days': retention_days,
            'cutoff_date': cutoff_timestamp,
            'found_records': len(old_tasks),
            'deleted_records': deleted_count,
            'failed_deletions': failed_deletions,
            'cleanup_timestamp': datetime.utcnow().isoformat()
        }
        
        logger.info(f"Cleanup completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Cleanup task failed: {e}")
        return {
            'status': 'failed',
            'error': str(e),
            'cleanup_timestamp': datetime.utcnow().isoformat()
        }


@celery_app.task(
    bind=True,
    name='app.tasks.monitoring.monitor_task_progress',
    max_retries=1
)
def monitor_task_progress(self, task_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Monitor progress of active tasks and generate progress report.
    
    Args:
        task_id: Optional specific task ID to monitor
        
    Returns:
        Dict with progress monitoring results
    """
    
    dynamodb = DynamoDBClient()
    
    try:
        logger.info(f"Starting task progress monitoring for task: {task_id or 'all active tasks'}")
        
        if task_id:
            # Monitor specific task
            task = dynamodb.get_task(task_id)
            if not task:
                return {
                    'status': 'error',
                    'message': f"Task {task_id} not found"
                }
            
            tasks_to_monitor = [task]
        else:
            # Monitor all active tasks
            response = dynamodb.table.scan(
                FilterExpression='#status IN (:pending, :in_progress, :processing_chunks, :initializing)',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={
                    ':pending': 'PENDING',
                    ':in_progress': 'IN_PROGRESS',
                    ':processing_chunks': 'PROCESSING_CHUNKS',
                    ':initializing': 'INITIALIZING'
                }
            )
            
            tasks_to_monitor = response.get('Items', [])
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = dynamodb.table.scan(
                    FilterExpression='#status IN (:pending, :in_progress, :processing_chunks, :initializing)',
                    ExpressionAttributeNames={'#status': 'status'},
                    ExpressionAttributeValues={
                        ':pending': 'PENDING',
                        ':in_progress': 'IN_PROGRESS',
                        ':processing_chunks': 'PROCESSING_CHUNKS',
                        ':initializing': 'INITIALIZING'
                    },
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                tasks_to_monitor.extend(response.get('Items', []))
        
        # Convert Decimal objects
        tasks_to_monitor = [dynamodb._convert_decimal(task) for task in tasks_to_monitor]
        
        # Analyze task progress
        progress_report = {
            'monitoring_timestamp': datetime.utcnow().isoformat(),
            'total_active_tasks': len(tasks_to_monitor),
            'task_summary': defaultdict(int),
            'tasks': []
        }
        
        for task in tasks_to_monitor:
            task_progress = analyze_task_progress(task)
            progress_report['tasks'].append(task_progress)
            progress_report['task_summary'][task['status']] += 1
        
        # Check for stuck tasks
        stuck_tasks = [
            task for task in progress_report['tasks']
            if task.get('stuck_warning', False)
        ]
        
        if stuck_tasks:
            progress_report['stuck_tasks'] = len(stuck_tasks)
            progress_report['warnings'] = [
                f"Task {task['task_id']} appears to be stuck (no progress in {task.get('hours_since_update', 'N/A')} hours)"
                for task in stuck_tasks
            ]
        
        logger.info(f"Progress monitoring completed. Active tasks: {len(tasks_to_monitor)}")
        return progress_report
        
    except Exception as e:
        logger.error(f"Task progress monitoring failed: {e}")
        return {
            'status': 'failed',
            'error': str(e),
            'monitoring_timestamp': datetime.utcnow().isoformat()
        }


@celery_app.task(
    bind=True,
    name='app.tasks.monitoring.send_task_metrics',
    max_retries=1
)
def send_task_metrics(self, time_period_hours: int = 24) -> Dict[str, Any]:
    """
    Generate and send task metrics for monitoring dashboard.
    
    Args:
        time_period_hours: Hours to look back for metrics
        
    Returns:
        Dict with metrics data
    """
    
    dynamodb = DynamoDBClient()
    
    try:
        logger.info(f"Generating task metrics for the last {time_period_hours} hours")
        
        # Calculate time range
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=time_period_hours)
        start_timestamp = start_time.isoformat()
        
        # Get tasks within time period
        response = dynamodb.table.scan(
            FilterExpression='updated_at >= :start_time',
            ExpressionAttributeValues={':start_time': start_timestamp}
        )
        
        tasks = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = dynamodb.table.scan(
                FilterExpression='updated_at >= :start_time',
                ExpressionAttributeValues={':start_time': start_timestamp},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            tasks.extend(response.get('Items', []))
        
        # Convert Decimal objects
        tasks = [dynamodb._convert_decimal(task) for task in tasks]
        
        # Calculate metrics
        metrics = calculate_task_metrics(tasks, time_period_hours)
        
        logger.info(f"Generated metrics for {len(tasks)} tasks")
        return metrics
        
    except Exception as e:
        logger.error(f"Task metrics generation failed: {e}")
        return {
            'status': 'failed',
            'error': str(e),
            'metrics_timestamp': datetime.utcnow().isoformat()
        }


@celery_app.task(
    bind=True,
    name='app.tasks.monitoring.detect_anomalies',
    max_retries=1
)
def detect_anomalies(self, lookback_hours: int = 24) -> Dict[str, Any]:
    """
    Detect anomalies in task processing patterns.
    
    Args:
        lookback_hours: Hours to look back for anomaly detection
        
    Returns:
        Dict with anomaly detection results
    """
    
    dynamodb = DynamoDBClient()
    
    try:
        logger.info(f"Starting anomaly detection for the last {lookback_hours} hours")
        
        # Get recent tasks
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=lookback_hours)
        start_timestamp = start_time.isoformat()
        
        response = dynamodb.table.scan(
            FilterExpression='updated_at >= :start_time',
            ExpressionAttributeValues={':start_time': start_timestamp}
        )
        
        tasks = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = dynamodb.table.scan(
                FilterExpression='updated_at >= :start_time',
                ExpressionAttributeValues={':start_time': start_timestamp},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            tasks.extend(response.get('Items', []))
        
        # Convert Decimal objects
        tasks = [dynamodb._convert_decimal(task) for task in tasks]
        
        # Detect anomalies
        anomalies = detect_task_anomalies(tasks, lookback_hours)
        
        logger.info(f"Anomaly detection completed. Found {len(anomalies.get('anomalies', []))} anomalies")
        return anomalies
        
    except Exception as e:
        logger.error(f"Anomaly detection failed: {e}")
        return {
            'status': 'failed',
            'error': str(e),
            'detection_timestamp': datetime.utcnow().isoformat()
        }


def analyze_task_progress(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze progress of a single task.
    
    Args:
        task: Task record from DynamoDB
        
    Returns:
        Dict with task progress analysis
    """
    
    now = datetime.utcnow()
    updated_at = datetime.fromisoformat(task['updated_at'].replace('Z', '+00:00'))
    hours_since_update = (now - updated_at).total_seconds() / 3600
    
    progress_info = {
        'task_id': task['task_id'],
        'status': task['status'],
        'task_type': task.get('task_type', 'unknown'),
        'created_at': task.get('created_at'),
        'updated_at': task['updated_at'],
        'hours_since_update': round(hours_since_update, 2),
        'total_records': task.get('total_records', 0),
        'processed_records': task.get('processed_records', 0),
        'failed_records': task.get('failed_records', 0)
    }
    
    # Calculate progress percentage
    total_records = task.get('total_records', 0)
    processed_records = task.get('processed_records', 0)
    
    if total_records > 0:
        progress_info['progress_percentage'] = round((processed_records / total_records) * 100, 2)
    else:
        progress_info['progress_percentage'] = 0
    
    # Check for stuck tasks (no update in over 2 hours for active tasks)
    if task['status'] in ['IN_PROGRESS', 'PROCESSING_CHUNKS'] and hours_since_update > 2:
        progress_info['stuck_warning'] = True
    
    # Add chunk information if available
    if 'total_chunks' in task:
        progress_info['chunk_info'] = {
            'total_chunks': task.get('total_chunks', 0),
            'completed_chunks': task.get('completed_chunks', 0),
            'failed_chunks': task.get('failed_chunks', 0)
        }
    
    return progress_info


def calculate_task_metrics(tasks: List[Dict[str, Any]], time_period_hours: int) -> Dict[str, Any]:
    """
    Calculate comprehensive task metrics.
    
    Args:
        tasks: List of task records
        time_period_hours: Time period for metrics calculation
        
    Returns:
        Dict with calculated metrics
    """
    
    metrics = {
        'metrics_timestamp': datetime.utcnow().isoformat(),
        'time_period_hours': time_period_hours,
        'total_tasks': len(tasks),
        'status_distribution': defaultdict(int),
        'type_distribution': defaultdict(int),
        'performance_metrics': {},
        'error_analysis': {}
    }
    
    # Basic distributions
    total_processed = 0
    total_failed = 0
    processing_times = []
    error_messages = []
    
    for task in tasks:
        # Status distribution
        metrics['status_distribution'][task.get('status', 'unknown')] += 1
        
        # Type distribution
        metrics['type_distribution'][task.get('task_type', 'unknown')] += 1
        
        # Processing metrics
        processed = task.get('processed_records', 0)
        failed = task.get('failed_records', 0)
        total_processed += processed
        total_failed += failed
        
        # Processing time calculation
        if task.get('completed_at') and task.get('created_at'):
            try:
                completed_at = datetime.fromisoformat(task['completed_at'].replace('Z', '+00:00'))
                created_at = datetime.fromisoformat(task['created_at'].replace('Z', '+00:00'))
                processing_time = (completed_at - created_at).total_seconds() / 60  # minutes
                processing_times.append(processing_time)
            except:
                pass
        
        # Error analysis
        if task.get('error_message'):
            error_messages.append(task['error_message'])
    
    # Performance metrics
    metrics['performance_metrics'] = {
        'total_records_processed': total_processed,
        'total_records_failed': total_failed,
        'success_rate_percentage': round((total_processed / (total_processed + total_failed)) * 100, 2) if (total_processed + total_failed) > 0 else 0,
        'average_processing_time_minutes': round(sum(processing_times) / len(processing_times), 2) if processing_times else 0,
        'min_processing_time_minutes': min(processing_times) if processing_times else 0,
        'max_processing_time_minutes': max(processing_times) if processing_times else 0
    }
    
    # Error analysis
    error_freq = defaultdict(int)
    for error in error_messages:
        # Simple error categorization
        error_key = error.split(':')[0] if ':' in error else error[:50]
        error_freq[error_key] += 1
    
    metrics['error_analysis'] = {
        'total_errors': len(error_messages),
        'unique_errors': len(error_freq),
        'common_errors': dict(sorted(error_freq.items(), key=lambda x: x[1], reverse=True)[:5])
    }
    
    return metrics


def detect_task_anomalies(tasks: List[Dict[str, Any]], lookback_hours: int) -> Dict[str, Any]:
    """
    Detect anomalies in task processing patterns.
    
    Args:
        tasks: List of task records
        lookback_hours: Hours of data to analyze
        
    Returns:
        Dict with anomaly detection results
    """
    
    anomalies = {
        'detection_timestamp': datetime.utcnow().isoformat(),
        'lookback_hours': lookback_hours,
        'total_tasks_analyzed': len(tasks),
        'anomalies': [],
        'summary': {}
    }
    
    # Anomaly detection thresholds
    HIGH_FAILURE_RATE_THRESHOLD = 0.15  # 15%
    STUCK_TASK_HOURS = 4
    UNUSUAL_PROCESSING_TIME_MULTIPLIER = 3
    
    # Calculate baseline metrics
    if not tasks:
        anomalies['summary'] = {'message': 'No tasks to analyze'}
        return anomalies
    
    # 1. High failure rate detection
    failed_tasks = [t for t in tasks if t.get('status') == 'FAILED']
    failure_rate = len(failed_tasks) / len(tasks)
    
    if failure_rate > HIGH_FAILURE_RATE_THRESHOLD:
        anomalies['anomalies'].append({
            'type': 'high_failure_rate',
            'severity': 'high',
            'description': f"Failure rate of {failure_rate:.2%} exceeds threshold of {HIGH_FAILURE_RATE_THRESHOLD:.2%}",
            'details': {
                'failed_tasks': len(failed_tasks),
                'total_tasks': len(tasks),
                'failure_rate': failure_rate
            }
        })
    
    # 2. Stuck task detection
    now = datetime.utcnow()
    stuck_tasks = []
    
    for task in tasks:
        if task.get('status') in ['IN_PROGRESS', 'PROCESSING_CHUNKS']:
            try:
                updated_at = datetime.fromisoformat(task['updated_at'].replace('Z', '+00:00'))
                hours_since_update = (now - updated_at).total_seconds() / 3600
                
                if hours_since_update > STUCK_TASK_HOURS:
                    stuck_tasks.append({
                        'task_id': task['task_id'],
                        'status': task['status'],
                        'hours_stuck': round(hours_since_update, 2)
                    })
            except:
                continue
    
    if stuck_tasks:
        anomalies['anomalies'].append({
            'type': 'stuck_tasks',
            'severity': 'medium',
            'description': f"Found {len(stuck_tasks)} tasks stuck for more than {STUCK_TASK_HOURS} hours",
            'details': {'stuck_tasks': stuck_tasks}
        })
    
    # 3. Processing time anomalies
    processing_times = []
    for task in tasks:
        if task.get('completed_at') and task.get('created_at'):
            try:
                completed_at = datetime.fromisoformat(task['completed_at'].replace('Z', '+00:00'))
                created_at = datetime.fromisoformat(task['created_at'].replace('Z', '+00:00'))
                processing_time = (completed_at - created_at).total_seconds() / 60  # minutes
                processing_times.append({
                    'task_id': task['task_id'],
                    'processing_time': processing_time
                })
            except:
                continue
    
    if processing_times:
        avg_time = sum(pt['processing_time'] for pt in processing_times) / len(processing_times)
        unusual_tasks = [
            pt for pt in processing_times
            if pt['processing_time'] > avg_time * UNUSUAL_PROCESSING_TIME_MULTIPLIER
        ]
        
        if unusual_tasks:
            anomalies['anomalies'].append({
                'type': 'unusual_processing_times',
                'severity': 'low',
                'description': f"Found {len(unusual_tasks)} tasks with unusually long processing times",
                'details': {
                    'average_time_minutes': round(avg_time, 2),
                    'threshold_multiplier': UNUSUAL_PROCESSING_TIME_MULTIPLIER,
                    'unusual_tasks': [
                        {
                            'task_id': ut['task_id'],
                            'processing_time_minutes': round(ut['processing_time'], 2)
                        }
                        for ut in unusual_tasks[:5]  # Limit to top 5
                    ]
                }
            })
    
    # Summary
    anomalies['summary'] = {
        'total_anomalies': len(anomalies['anomalies']),
        'severity_counts': {
            'high': len([a for a in anomalies['anomalies'] if a['severity'] == 'high']),
            'medium': len([a for a in anomalies['anomalies'] if a['severity'] == 'medium']),
            'low': len([a for a in anomalies['anomalies'] if a['severity'] == 'low'])
        }
    }
    
    return anomalies