#!/bin/bash

# Celery workers startup script for VectorDB API

set -e

echo "Starting VectorDB Celery Workers..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
ENVIRONMENT=${ENVIRONMENT:-development}
REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_DB=${REDIS_DB:-0}
WORKERS=${WORKERS:-4}
LOG_LEVEL=${LOG_LEVEL:-INFO}
MAX_TASKS_PER_CHILD=${MAX_TASKS_PER_CHILD:-1000}
MAX_MEMORY_PER_CHILD=${MAX_MEMORY_PER_CHILD:-200000}

# Project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "Project root: $PROJECT_ROOT"
echo "Environment: $ENVIRONMENT"
echo "Redis: ${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Virtual environment not found. Creating one...${NC}"
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies if needed
if [ ! -f "venv/lib/python*/site-packages/celery/__init__.py" ]; then
    echo -e "${YELLOW}Installing dependencies...${NC}"
    pip install -r requirements.txt
fi

# Check if Redis is running
echo "Checking Redis connection..."
if ! python3 -c "import redis; r=redis.Redis(host='$REDIS_HOST', port=$REDIS_PORT, db=$REDIS_DB); r.ping()" 2>/dev/null; then
    echo -e "${RED}Cannot connect to Redis at ${REDIS_HOST}:${REDIS_PORT}${NC}"
    echo "Please start Redis first using: ./scripts/start_redis.sh"
    exit 1
fi

echo -e "${GREEN}Redis connection successful${NC}"

# Create logs directory
mkdir -p logs

# Function to start a worker
start_worker() {
    local worker_name=$1
    local queue=$2
    local concurrency=$3
    local log_file="logs/worker_${worker_name}.log"
    
    echo -e "${BLUE}Starting worker: $worker_name (queue: $queue, concurrency: $concurrency)${NC}"
    
    nohup python worker.py \
        --app=app.celery_config:celery_app \
        --hostname="${worker_name}@%h" \
        --queues="$queue" \
        --concurrency="$concurrency" \
        --loglevel="$LOG_LEVEL" \
        --max-tasks-per-child="$MAX_TASKS_PER_CHILD" \
        --max-memory-per-child="$MAX_MEMORY_PER_CHILD" \
        --optimization=fair \
        --prefetch-multiplier=1 \
        --without-gossip \
        --without-mingle \
        --without-heartbeat \
        > "$log_file" 2>&1 &
    
    local pid=$!
    echo $pid > "logs/worker_${worker_name}.pid"
    echo "Worker $worker_name started with PID $pid (log: $log_file)"
}

# Function to start Flower monitoring
start_flower() {
    echo -e "${BLUE}Starting Flower monitoring dashboard...${NC}"
    
    nohup celery flower \
        --app=app.celery_config:celery_app \
        --port=5555 \
        --broker="redis://${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}" \
        --url_prefix=flower \
        --basic_auth=admin:vectordb \
        > logs/flower.log 2>&1 &
    
    local pid=$!
    echo $pid > logs/flower.pid
    echo "Flower started with PID $pid (http://localhost:5555)"
}

# Start workers for different queues
echo ""
echo "Starting Celery workers..."

# Data processing workers (main workload)
start_worker "data_processor_1" "data_processing" "$WORKERS"
start_worker "data_processor_2" "data_processing" "$WORKERS"

# Chunk processing workers (parallel chunk processing)
chunk_workers=$((WORKERS * 2))  # More workers for chunk processing
start_worker "chunk_processor_1" "chunk_processing" "$chunk_workers"
start_worker "chunk_processor_2" "chunk_processing" "$chunk_workers"

# Monitoring workers (lightweight)
start_worker "monitor" "monitoring" "2"

# Start Flower monitoring if not in production
if [ "$ENVIRONMENT" != "production" ]; then
    start_flower
fi

# Wait a moment for workers to start
sleep 3

# Check worker status
echo ""
echo -e "${GREEN}Checking worker status...${NC}"

# List active workers
python3 -c "
from app.celery_config import celery_app
import sys
inspect = celery_app.control.inspect()
stats = inspect.stats()
if stats:
    print('Active workers:')
    for worker, stat in stats.items():
        print(f'  - {worker}: OK')
else:
    print('No workers found')
    sys.exit(1)
"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}All workers started successfully!${NC}"
else
    echo -e "${RED}Some workers failed to start${NC}"
fi

echo ""
echo "Worker management commands:"
echo "  View status: ./scripts/worker_status.sh"
echo "  Stop all:    ./scripts/stop_workers.sh"
echo "  Restart:     ./scripts/restart_workers.sh"

if [ "$ENVIRONMENT" != "production" ]; then
    echo ""
    echo -e "${GREEN}Flower monitoring: http://localhost:5555${NC}"
    echo "  Username: admin"
    echo "  Password: vectordb"
fi

echo ""
echo "Log files location: logs/"
echo "  - Worker logs: logs/worker_*.log"
echo "  - Flower log: logs/flower.log"

echo ""
echo -e "${GREEN}VectorDB Celery workers are now running!${NC}"
echo "You can now start the API server: uvicorn app.main:app --host 0.0.0.0 --port 8000"