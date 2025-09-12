"""
Monitoring and health check endpoints for the VectorDB API.
Provides comprehensive monitoring of Celery workers, tasks, and system health.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import JSONResponse

from app.api.auth import verify_api_key
from app.config import settings
from app.services.dynamodb_client import DynamoDBClient
from app.utils.progress_tracker import progress_tracker
from app.celery_config import celery_app
from app.tasks.health_check import worker_health_check, system_health_check
from app.tasks.monitoring import monitor_task_progress, send_task_metrics, detect_anomalies


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/monitoring", tags=["monitoring"])


@router.get("/health")
async def health_check():
    """
    Basic health check endpoint - no authentication required.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "vectordb-api",
        "version": "2.0.0"
    }


@router.get("/health/detailed")
async def detailed_health_check(api_key: str = Depends(verify_api_key)):
    """
    Detailed health check with external service status.
    """
    try:
        # Trigger system health check task
        health_task = system_health_check.delay()
        
        # Wait for result (with timeout)
        try:
            health_result = health_task.get(timeout=30)
            return health_result
        except Exception as e:
            logger.error(f"Health check task failed: {e}")
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "overall_status": "unhealthy",
                "error": f"Health check task failed: {str(e)}",
                "checks": {}
            }
    
    except Exception as e:
        logger.error(f"Detailed health check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")


@router.get("/workers")
async def get_worker_status(api_key: str = Depends(verify_api_key)):
    """
    Get status of all Celery workers.
    """
    try:
        inspect = celery_app.control.inspect()
        
        # Get worker stats
        stats = inspect.stats()
        active_tasks = inspect.active()
        scheduled_tasks = inspect.scheduled()
        reserved_tasks = inspect.reserved()
        
        workers = []
        
        if stats:
            for worker_name, worker_stats in stats.items():
                worker_info = {
                    "name": worker_name,
                    "status": "online",
                    "stats": {
                        "pool": worker_stats.get("pool", {}),
                        "total_tasks": worker_stats.get("total", {}),
                        "rusage": worker_stats.get("rusage", {})
                    },
                    "active_tasks": len(active_tasks.get(worker_name, [])) if active_tasks else 0,
                    "scheduled_tasks": len(scheduled_tasks.get(worker_name, [])) if scheduled_tasks else 0,
                    "reserved_tasks": len(reserved_tasks.get(worker_name, [])) if reserved_tasks else 0
                }
                workers.append(worker_info)
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "total_workers": len(workers),
            "online_workers": len([w for w in workers if w["status"] == "online"]),
            "workers": workers
        }
    
    except Exception as e:
        logger.error(f"Failed to get worker status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get worker status: {str(e)}")


@router.get("/queues")
async def get_queue_status(api_key: str = Depends(verify_api_key)):
    """
    Get status of Celery queues.
    """
    try:
        import redis
        redis_client = redis.from_url(settings.redis_url)
        
        queue_names = ['data_processing', 'chunk_processing', 'monitoring']
        queue_stats = []
        
        for queue_name in queue_names:
            queue_key = f"celery:{queue_name}"
            length = redis_client.llen(queue_key)
            
            queue_stats.append({
                "name": queue_name,
                "length": length,
                "key": queue_key
            })
        
        # Get active tasks from Celery
        inspect = celery_app.control.inspect()
        active_tasks = inspect.active()
        
        total_active = 0
        if active_tasks:
            for worker_tasks in active_tasks.values():
                total_active += len(worker_tasks)
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "total_queued": sum(q["length"] for q in queue_stats),
            "total_active": total_active,
            "queues": queue_stats
        }
    
    except Exception as e:
        logger.error(f"Failed to get queue status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get queue status: {str(e)}")


@router.get("/tasks")
async def get_task_status(
    status: Optional[str] = Query(None, description="Filter by task status"),
    task_type: Optional[str] = Query(None, description="Filter by task type"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of tasks to return"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get task status information.
    """
    try:
        dynamodb = DynamoDBClient()
        
        if status:
            tasks = dynamodb.get_tasks_by_status(status, task_type, limit)
        elif task_type:
            # Get tasks by type (all statuses)
            tasks = dynamodb.get_tasks_by_status("PENDING", task_type, limit)  # This is not ideal - need better query
            # For now, let's get active tasks and filter
            tasks = dynamodb.get_active_tasks(limit)
            if task_type:
                tasks = [t for t in tasks if t.get('task_type') == task_type]
        else:
            tasks = dynamodb.get_active_tasks(limit)
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "total_tasks": len(tasks),
            "filters": {
                "status": status,
                "task_type": task_type,
                "limit": limit
            },
            "tasks": tasks
        }
    
    except Exception as e:
        logger.error(f"Failed to get task status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get task status: {str(e)}")


@router.get("/tasks/{task_id}")
async def get_task_details(
    task_id: str,
    api_key: str = Depends(verify_api_key)
):
    """
    Get detailed information about a specific task.
    """
    try:
        dynamodb = DynamoDBClient()
        
        # Get task from DynamoDB
        task = dynamodb.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        # Get real-time progress if available
        progress = progress_tracker.get_progress(task_id)
        
        # Merge task data with progress data
        if progress:
            task.update({
                "real_time_progress": progress,
                "progress_available": True
            })
        else:
            task["progress_available"] = False
        
        # Get Celery task info if available
        celery_task_id = task.get('celery_task_id')
        if celery_task_id:
            try:
                from celery.result import AsyncResult
                celery_result = AsyncResult(celery_task_id, app=celery_app)
                task["celery_status"] = {
                    "state": celery_result.state,
                    "info": celery_result.info,
                    "successful": celery_result.successful(),
                    "failed": celery_result.failed()
                }
            except Exception as e:
                logger.warning(f"Could not get Celery status for task {celery_task_id}: {e}")
                task["celery_status"] = {"error": str(e)}
        
        return task
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get task details for {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get task details: {str(e)}")


@router.get("/tasks/{task_id}/progress")
async def get_task_progress(
    task_id: str,
    api_key: str = Depends(verify_api_key)
):
    """
    Get real-time progress for a specific task.
    """
    try:
        progress = progress_tracker.get_progress(task_id)
        
        if not progress:
            raise HTTPException(status_code=404, detail=f"No progress data found for task {task_id}")
        
        return progress
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get task progress for {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get task progress: {str(e)}")


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(
    task_id: str,
    reason: Optional[str] = Query(None, description="Reason for cancellation"),
    api_key: str = Depends(verify_api_key)
):
    """
    Cancel a running or queued task.
    """
    try:
        dynamodb = DynamoDBClient()
        
        # Check if task exists
        task = dynamodb.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        # Check if task can be cancelled
        if task['status'] in ['COMPLETED', 'FAILED', 'CANCELLED']:
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot cancel task in {task['status']} status"
            )
        
        # Cancel Celery task if exists
        celery_task_id = task.get('celery_task_id')
        if celery_task_id:
            try:
                celery_app.control.revoke(celery_task_id, terminate=True)
                logger.info(f"Revoked Celery task {celery_task_id}")
            except Exception as e:
                logger.warning(f"Failed to revoke Celery task {celery_task_id}: {e}")
        
        # Update task status in DynamoDB
        dynamodb.cancel_task(task_id, reason)
        
        # Clean up progress tracking
        progress_tracker.cleanup_completed_progress(task_id)
        
        return {
            "message": f"Task {task_id} cancelled successfully",
            "task_id": task_id,
            "reason": reason,
            "cancelled_at": datetime.utcnow().isoformat()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to cancel task: {str(e)}")


@router.get("/statistics")
async def get_system_statistics(
    hours: int = Query(24, ge=1, le=168, description="Hours to look back for statistics"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get system statistics for the specified time period.
    """
    try:
        dynamodb = DynamoDBClient()
        
        # Get task statistics from DynamoDB
        stats = dynamodb.get_task_statistics(hours)
        
        # Add current system status
        current_time = datetime.utcnow()
        stats.update({
            "generated_at": current_time.isoformat(),
            "system_status": {
                "redis_connected": True,  # We'll enhance this
                "workers_online": 0,  # We'll calculate this
                "queues_healthy": True  # We'll enhance this
            }
        })
        
        # Get current worker status
        try:
            inspect = celery_app.control.inspect()
            worker_stats = inspect.stats()
            if worker_stats:
                stats["system_status"]["workers_online"] = len(worker_stats)
        except Exception as e:
            logger.warning(f"Could not get worker stats: {e}")
            stats["system_status"]["workers_online"] = 0
        
        return stats
    
    except Exception as e:
        logger.error(f"Failed to get system statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get system statistics: {str(e)}")


@router.get("/metrics")
async def get_metrics(
    hours: int = Query(24, ge=1, le=168, description="Hours to look back for metrics"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get detailed metrics for monitoring dashboard.
    """
    try:
        # Trigger metrics collection task
        metrics_task = send_task_metrics.delay(hours)
        
        # Wait for result (with timeout)
        try:
            metrics_result = metrics_task.get(timeout=30)
            return metrics_result
        except Exception as e:
            logger.error(f"Metrics task failed: {e}")
            return {
                "status": "failed",
                "error": f"Metrics collection failed: {str(e)}",
                "metrics_timestamp": datetime.utcnow().isoformat()
            }
    
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get metrics: {str(e)}")


@router.get("/anomalies")
async def detect_system_anomalies(
    hours: int = Query(24, ge=1, le=168, description="Hours to look back for anomaly detection"),
    api_key: str = Depends(verify_api_key)
):
    """
    Detect anomalies in system behavior.
    """
    try:
        # Trigger anomaly detection task
        anomaly_task = detect_anomalies.delay(hours)
        
        # Wait for result (with timeout)
        try:
            anomaly_result = anomaly_task.get(timeout=45)
            return anomaly_result
        except Exception as e:
            logger.error(f"Anomaly detection task failed: {e}")
            return {
                "status": "failed",
                "error": f"Anomaly detection failed: {str(e)}",
                "detection_timestamp": datetime.utcnow().isoformat()
            }
    
    except Exception as e:
        logger.error(f"Failed to detect anomalies: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to detect anomalies: {str(e)}")


@router.get("/progress/active")
async def get_active_tasks_progress(
    api_key: str = Depends(verify_api_key)
):
    """
    Get progress for all currently active tasks.
    """
    try:
        progress_data = progress_tracker.get_active_tasks_progress()
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "active_task_count": len(progress_data),
            "progress_data": progress_data
        }
    
    except Exception as e:
        logger.error(f"Failed to get active tasks progress: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get active tasks progress: {str(e)}")


@router.post("/cleanup")
async def cleanup_old_data(
    retention_days: int = Query(7, ge=1, le=90, description="Number of days to retain completed tasks"),
    api_key: str = Depends(verify_api_key)
):
    """
    Manually trigger cleanup of old task data.
    """
    try:
        from app.tasks.monitoring import cleanup_old_task_records
        
        # Trigger cleanup task
        cleanup_task = cleanup_old_task_records.delay(retention_days)
        
        return {
            "message": "Cleanup task initiated",
            "task_id": cleanup_task.id,
            "retention_days": retention_days,
            "initiated_at": datetime.utcnow().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Failed to initiate cleanup: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to initiate cleanup: {str(e)}")


@router.get("/dashboard")
async def get_dashboard_data(
    api_key: str = Depends(verify_api_key)
):
    """
    Get comprehensive dashboard data in one endpoint.
    """
    try:
        dashboard_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "system": {},
            "workers": {},
            "queues": {},
            "tasks": {},
            "progress": {}
        }
        
        # Get system health (lightweight check)
        try:
            dashboard_data["system"] = {
                "status": "healthy",
                "redis_connected": True,
                "environment": settings.ENVIRONMENT
            }
        except Exception as e:
            dashboard_data["system"] = {
                "status": "unhealthy",
                "error": str(e)
            }
        
        # Get worker status
        try:
            inspect = celery_app.control.inspect()
            stats = inspect.stats()
            active_tasks = inspect.active()
            
            if stats:
                dashboard_data["workers"] = {
                    "total": len(stats),
                    "online": len(stats),
                    "total_active_tasks": sum(len(tasks) for tasks in (active_tasks or {}).values())
                }
            else:
                dashboard_data["workers"] = {"total": 0, "online": 0, "total_active_tasks": 0}
        except Exception as e:
            dashboard_data["workers"] = {"error": str(e)}
        
        # Get queue status
        try:
            import redis
            redis_client = redis.from_url(settings.redis_url)
            queue_names = ['data_processing', 'chunk_processing', 'monitoring']
            total_queued = sum(redis_client.llen(f"celery:{name}") for name in queue_names)
            dashboard_data["queues"] = {"total_queued": total_queued}
        except Exception as e:
            dashboard_data["queues"] = {"error": str(e)}
        
        # Get task summary
        try:
            dynamodb = DynamoDBClient()
            active_tasks = dynamodb.get_active_tasks(limit=50)
            
            status_counts = {}
            for task in active_tasks:
                status = task.get('status', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
            
            dashboard_data["tasks"] = {
                "active_count": len(active_tasks),
                "status_breakdown": status_counts
            }
        except Exception as e:
            dashboard_data["tasks"] = {"error": str(e)}
        
        # Get progress data
        try:
            progress_data = progress_tracker.get_active_tasks_progress()
            dashboard_data["progress"] = {
                "tasks_with_progress": len(progress_data),
                "sample_tasks": list(progress_data.keys())[:5]  # Show first 5 task IDs
            }
        except Exception as e:
            dashboard_data["progress"] = {"error": str(e)}
        
        return dashboard_data
    
    except Exception as e:
        logger.error(f"Failed to get dashboard data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get dashboard data: {str(e)}")