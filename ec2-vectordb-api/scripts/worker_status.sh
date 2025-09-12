#!/bin/bash

# Check status of VectorDB Celery Workers

echo "VectorDB Celery Workers Status"
echo "=============================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Check if logs directory exists
if [ ! -d "logs" ]; then
    echo -e "${RED}Logs directory not found. Workers may not be running.${NC}"
    exit 1
fi

# Function to check process status
check_process() {
    local name=$1
    local pid_file="logs/${name}.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p $pid > /dev/null 2>&1; then
            local cpu_mem=$(ps -p $pid -o %cpu,%mem --no-headers | xargs)
            echo -e "${GREEN}✓ $name (PID: $pid, CPU/MEM: $cpu_mem)${NC}"
            return 0
        else
            echo -e "${RED}✗ $name (PID: $pid - not running)${NC}"
            return 1
        fi
    else
        echo -e "${YELLOW}? $name (no PID file)${NC}"
        return 1
    fi
}

# Check individual workers
echo ""
echo "Worker Processes:"
running_workers=0
total_workers=0

for pid_file in logs/worker_*.pid; do
    if [ -f "$pid_file" ]; then
        worker_name=$(basename "$pid_file" .pid)
        ((total_workers++))
        if check_process "$worker_name"; then
            ((running_workers++))
        fi
    fi
done

# Check Flower
echo ""
echo "Monitoring:"
if check_process "flower"; then
    echo -e "  ${BLUE}Flower dashboard: http://localhost:5555${NC}"
fi

# Redis status
echo ""
echo "Redis Status:"
if command -v redis-cli &> /dev/null; then
    if redis-cli ping > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Redis is running and responding${NC}"
        
        # Get Redis info
        redis_version=$(redis-cli INFO server | grep redis_version | cut -d: -f2 | tr -d '\r')
        redis_memory=$(redis-cli INFO memory | grep used_memory_human | cut -d: -f2 | tr -d '\r')
        echo "  Version: $redis_version"
        echo "  Memory usage: $redis_memory"
    else
        echo -e "${RED}✗ Redis is not responding${NC}"
    fi
else
    echo -e "${YELLOW}? redis-cli not found${NC}"
fi

# Celery status using Python
echo ""
echo "Celery Cluster Status:"
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    python3 -c "
from app.celery_config import celery_app
import sys

print('Checking Celery workers...')
try:
    inspect = celery_app.control.inspect()
    
    # Get worker stats
    stats = inspect.stats()
    active_tasks = inspect.active()
    reserved_tasks = inspect.reserved()
    scheduled_tasks = inspect.scheduled()
    
    if stats:
        print(f'Active workers: {len(stats)}')
        for worker, stat in stats.items():
            total_tasks = stat.get('total', {})
            pool_info = stat.get('pool', {})
            print(f'  - {worker}:')
            print(f'    Pool: {pool_info.get(\"max-concurrency\", \"N/A\")} max concurrency')
            print(f'    Tasks: {len(active_tasks.get(worker, []))} active, {len(reserved_tasks.get(worker, []))} reserved')
    else:
        print('No workers found')
        sys.exit(1)
        
    # Queue status
    import redis
    from app.config import settings
    r = redis.from_url(settings.redis_url)
    
    print('')
    print('Queue Status:')
    queues = ['data_processing', 'chunk_processing', 'monitoring']
    total_queued = 0
    for queue in queues:
        length = r.llen(f'celery:{queue}')
        total_queued += length
        print(f'  - {queue}: {length} queued')
    
    print(f'Total queued tasks: {total_queued}')
    
except Exception as e:
    print(f'Error checking Celery status: {e}')
    sys.exit(1)
" 2>/dev/null
else
    echo -e "${YELLOW}Virtual environment not found. Skipping detailed Celery status.${NC}"
fi

echo ""
echo "Summary:"
echo "========="
if [ $running_workers -gt 0 ]; then
    echo -e "${GREEN}Workers running: $running_workers/$total_workers${NC}"
else
    echo -e "${RED}No workers running${NC}"
fi

# Show recent log entries
echo ""
echo "Recent Log Entries (last 5 lines from each worker):"
echo "=================================================="
for log_file in logs/worker_*.log; do
    if [ -f "$log_file" ]; then
        worker_name=$(basename "$log_file" .log)
        echo ""
        echo -e "${BLUE}$worker_name:${NC}"
        tail -n 5 "$log_file" 2>/dev/null || echo "  (no log entries)"
    fi
done

echo ""
echo "Commands:"
echo "  Start workers: ./scripts/start_workers.sh"
echo "  Stop workers:  ./scripts/stop_workers.sh"
echo "  Restart:       ./scripts/restart_workers.sh"