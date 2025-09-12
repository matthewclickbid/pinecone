#!/bin/bash

# Restart all VectorDB Celery Workers

echo "Restarting VectorDB Celery Workers..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo -e "${YELLOW}Stopping existing workers...${NC}"
./scripts/stop_workers.sh

echo ""
echo -e "${YELLOW}Waiting 3 seconds...${NC}"
sleep 3

echo ""
echo -e "${YELLOW}Starting workers...${NC}"
./scripts/start_workers.sh

echo ""
echo -e "${GREEN}Workers restarted successfully!${NC}"