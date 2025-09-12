#!/bin/bash

# Stop all Celery workers and Flower for VectorDB API

set -e

echo "Stopping VectorDB Celery Workers..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Function to stop process by PID file
stop_process() {
    local name=$1
    local pid_file="logs/${name}.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p $pid > /dev/null 2>&1; then
            echo "Stopping $name (PID: $pid)..."
            kill -TERM $pid
            
            # Wait for graceful shutdown
            for i in {1..10}; do
                if ! ps -p $pid > /dev/null 2>&1; then
                    echo "$name stopped gracefully"
                    break
                fi
                sleep 1
            done
            
            # Force kill if still running
            if ps -p $pid > /dev/null 2>&1; then
                echo "Force killing $name..."
                kill -KILL $pid
            fi
        else
            echo "$name was not running (PID $pid not found)"
        fi
        rm -f "$pid_file"
    else
        echo "No PID file found for $name"
    fi
}

# Stop all workers
echo "Stopping Celery workers..."
for pid_file in logs/worker_*.pid; do
    if [ -f "$pid_file" ]; then
        worker_name=$(basename "$pid_file" .pid)
        stop_process "$worker_name"
    fi
done

# Stop Flower
if [ -f "logs/flower.pid" ]; then
    echo "Stopping Flower monitoring..."
    stop_process "flower"
fi

# Additional cleanup - kill any remaining celery processes
echo "Cleaning up any remaining Celery processes..."
pkill -f "celery.*vectordb" 2>/dev/null || true
pkill -f "worker.py" 2>/dev/null || true
pkill -f "celery flower" 2>/dev/null || true

# Clean up PID files
rm -f logs/*.pid

echo -e "${GREEN}All workers and monitoring stopped successfully!${NC}"

# Check if any processes are still running
if pgrep -f "celery.*vectordb" > /dev/null; then
    echo -e "${YELLOW}Warning: Some Celery processes may still be running${NC}"
    echo "Running Celery processes:"
    pgrep -fl "celery.*vectordb" || true
else
    echo -e "${GREEN}No Celery processes found running${NC}"
fi