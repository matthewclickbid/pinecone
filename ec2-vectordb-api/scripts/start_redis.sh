#!/bin/bash

# Redis setup and start script for VectorDB API

set -e

echo "Setting up Redis for VectorDB API..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_PASSWORD=${REDIS_PASSWORD:-}
REDIS_CONFIG_DIR="/etc/redis"
REDIS_DATA_DIR="/var/lib/redis"
REDIS_LOG_DIR="/var/log/redis"

# Check if Redis is installed
if ! command -v redis-server &> /dev/null; then
    echo -e "${YELLOW}Redis not found. Installing Redis...${NC}"
    
    # Detect OS
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux (Ubuntu/Debian)
        if command -v apt-get &> /dev/null; then
            sudo apt-get update
            sudo apt-get install -y redis-server
        # Linux (CentOS/RHEL/Amazon Linux)
        elif command -v yum &> /dev/null; then
            sudo yum update -y
            sudo yum install -y redis
        elif command -v dnf &> /dev/null; then
            sudo dnf install -y redis
        else
            echo -e "${RED}Unsupported Linux distribution${NC}"
            exit 1
        fi
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            brew install redis
        else
            echo -e "${RED}Homebrew not found. Please install Redis manually.${NC}"
            exit 1
        fi
    else
        echo -e "${RED}Unsupported operating system${NC}"
        exit 1
    fi
fi

# Create Redis configuration
echo "Creating Redis configuration..."

# Create directories
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    sudo mkdir -p $REDIS_CONFIG_DIR
    sudo mkdir -p $REDIS_DATA_DIR
    sudo mkdir -p $REDIS_LOG_DIR
fi

# Redis configuration for VectorDB
REDIS_CONF_CONTENT="# Redis configuration for VectorDB API
port $REDIS_PORT
bind 127.0.0.1
protected-mode yes
timeout 300
tcp-keepalive 300
daemonize yes
supervised systemd
pidfile /var/run/redis/redis-server.pid
loglevel notice
logfile $REDIS_LOG_DIR/redis-server.log
databases 16
dir $REDIS_DATA_DIR

# Memory management
maxmemory 1gb
maxmemory-policy allkeys-lru

# Persistence
save 900 1
save 300 10
save 60 10000
stop-writes-on-bgsave-error yes
rdbcompression yes
rdbchecksum yes
dbfilename vectordb-redis.rdb

# Append only file
appendonly yes
appendfilename \"vectordb-redis.aof\"
appendfsync everysec
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb

# Security
requirepass ${REDIS_PASSWORD:-vectordb_redis_pass}
"

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "$REDIS_CONF_CONTENT" | sudo tee $REDIS_CONFIG_DIR/vectordb-redis.conf > /dev/null
    REDIS_CONF_PATH="$REDIS_CONFIG_DIR/vectordb-redis.conf"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    echo "$REDIS_CONF_CONTENT" > ./redis.conf
    REDIS_CONF_PATH="./redis.conf"
fi

echo -e "${GREEN}Redis configuration created at $REDIS_CONF_PATH${NC}"

# Start Redis
echo "Starting Redis server..."

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux - use systemd if available
    if command -v systemctl &> /dev/null; then
        # Create systemd service
        REDIS_SERVICE_CONTENT="[Unit]
Description=VectorDB Redis Server
After=network.target

[Service]
Type=forking
ExecStart=/usr/bin/redis-server $REDIS_CONF_PATH
ExecReload=/bin/kill -USR2 \$MAINPID
ExecStop=/usr/bin/redis-cli shutdown
User=redis
Group=redis
RuntimeDirectory=redis
RuntimeDirectoryMode=0755

[Install]
WantedBy=multi-user.target"

        echo "$REDIS_SERVICE_CONTENT" | sudo tee /etc/systemd/system/vectordb-redis.service > /dev/null
        
        # Set permissions
        sudo chown -R redis:redis $REDIS_DATA_DIR $REDIS_LOG_DIR
        
        # Start service
        sudo systemctl daemon-reload
        sudo systemctl enable vectordb-redis
        sudo systemctl start vectordb-redis
        
        echo -e "${GREEN}Redis started as systemd service${NC}"
    else
        # Start Redis directly
        redis-server $REDIS_CONF_PATH &
        echo -e "${GREEN}Redis started in background${NC}"
    fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    redis-server ./redis.conf &
    echo -e "${GREEN}Redis started in background${NC}"
fi

# Wait for Redis to start
sleep 2

# Test Redis connection
echo "Testing Redis connection..."
if redis-cli -p $REDIS_PORT ping > /dev/null 2>&1; then
    echo -e "${GREEN}Redis is running and responding to connections${NC}"
else
    echo -e "${RED}Failed to connect to Redis${NC}"
    exit 1
fi

# Show Redis info
echo ""
echo "Redis Configuration:"
echo "  Port: $REDIS_PORT"
echo "  Config: $REDIS_CONF_PATH"
echo "  Status: Running"

if [[ "$OSTYPE" == "linux-gnu"* ]] && command -v systemctl &> /dev/null; then
    echo ""
    echo "Redis service commands:"
    echo "  Start:   sudo systemctl start vectordb-redis"
    echo "  Stop:    sudo systemctl stop vectordb-redis"
    echo "  Restart: sudo systemctl restart vectordb-redis"
    echo "  Status:  sudo systemctl status vectordb-redis"
    echo "  Logs:    sudo journalctl -u vectordb-redis -f"
fi

echo ""
echo -e "${GREEN}Redis setup complete!${NC}"
echo "You can now start the Celery workers and API server."