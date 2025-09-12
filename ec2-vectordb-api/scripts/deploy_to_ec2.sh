#!/bin/bash

# EC2 Deployment Script for VectorDB API
# This script deploys the application to an AWS EC2 instance

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
APP_NAME="vectordb-api"
APP_DIR="/home/ubuntu/vectordb-api"
REPO_URL="https://github.com/your-org/ec2-vectordb-api.git"  # Update this
BRANCH="main"
ENV_FILE=".env.production"
BACKUP_DIR="/home/ubuntu/backups"

# Function to print colored output
print_status() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to backup current deployment
backup_current() {
    if [ -d "$APP_DIR" ]; then
        print_status "Creating backup of current deployment..."
        mkdir -p $BACKUP_DIR
        BACKUP_NAME="backup-$(date +'%Y%m%d-%H%M%S').tar.gz"
        tar -czf "$BACKUP_DIR/$BACKUP_NAME" -C "$(dirname $APP_DIR)" "$(basename $APP_DIR)" 2>/dev/null || true
        print_status "Backup created: $BACKUP_DIR/$BACKUP_NAME"
        
        # Keep only last 5 backups
        ls -t $BACKUP_DIR/backup-*.tar.gz | tail -n +6 | xargs rm -f 2>/dev/null || true
    fi
}

# Function to install dependencies
install_dependencies() {
    print_status "Installing system dependencies..."
    
    # Update system
    sudo apt-get update -y
    
    # Install required packages
    sudo apt-get install -y \
        docker.io \
        docker-compose \
        git \
        jq \
        awscli \
        python3-pip \
        htop \
        nginx
    
    # Add user to docker group
    sudo usermod -aG docker $USER
    
    # Start Docker service
    sudo systemctl start docker
    sudo systemctl enable docker
}

# Function to setup CloudWatch agent
setup_cloudwatch() {
    print_status "Setting up CloudWatch agent..."
    
    # Download and install CloudWatch agent
    wget https://s3.amazonaws.com/amazoncloudwatch-agent/ubuntu/amd64/latest/amazon-cloudwatch-agent.deb
    sudo dpkg -i -E ./amazon-cloudwatch-agent.deb
    rm amazon-cloudwatch-agent.deb
    
    # Create CloudWatch agent configuration
    cat <<EOF | sudo tee /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "$APP_DIR/logs/app.log",
            "log_group_name": "/aws/ec2/vectordb-api",
            "log_stream_name": "{instance_id}/app"
          },
          {
            "file_path": "$APP_DIR/logs/worker.log",
            "log_group_name": "/aws/ec2/vectordb-worker",
            "log_stream_name": "{instance_id}/worker"
          }
        ]
      }
    }
  }
}
EOF
    
    # Start CloudWatch agent
    sudo systemctl start amazon-cloudwatch-agent
    sudo systemctl enable amazon-cloudwatch-agent
}

# Function to get secrets from AWS Secrets Manager
get_secrets() {
    print_status "Retrieving secrets from AWS Secrets Manager..."
    
    # Get secrets and create .env file
    aws secretsmanager get-secret-value \
        --secret-id "vectordb-api-secrets" \
        --query SecretString \
        --output text \
        --region us-east-1 > $APP_DIR/$ENV_FILE
    
    if [ $? -eq 0 ]; then
        print_status "Secrets retrieved successfully"
    else
        print_error "Failed to retrieve secrets from AWS Secrets Manager"
        exit 1
    fi
}

# Function to deploy application
deploy_application() {
    print_status "Starting deployment of $APP_NAME..."
    
    # Create backup
    backup_current
    
    # Clone or update repository
    if [ -d "$APP_DIR/.git" ]; then
        print_status "Updating existing repository..."
        cd $APP_DIR
        git fetch origin
        git checkout $BRANCH
        git pull origin $BRANCH
    else
        print_status "Cloning repository..."
        sudo rm -rf $APP_DIR
        git clone -b $BRANCH $REPO_URL $APP_DIR
        cd $APP_DIR
    fi
    
    # Clean up unnecessary files
    print_status "Cleaning up unnecessary files..."
    rm -f check_pinecone.py simple_server.py test_*.py verify_vectors.py process_full_csv.py worker.py
    rm -rf venv/ venv_test/ __pycache__/
    rm -f .env.local DEPLOYMENT_PHASE2.md
    find . -type f -name "*.pyc" -delete
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    
    # Get secrets
    get_secrets
    
    # Create necessary directories
    mkdir -p logs
    
    # Build and start Docker containers
    print_status "Building Docker images..."
    docker-compose build
    
    print_status "Stopping existing containers..."
    docker-compose down
    
    print_status "Starting new containers..."
    docker-compose up -d
    
    # Wait for services to be healthy
    print_status "Waiting for services to be healthy..."
    sleep 10
    
    # Check health
    check_health
}

# Function to check application health
check_health() {
    print_status "Checking application health..."
    
    # Check API health
    HEALTH_URL="http://localhost:8000/api/v1/health"
    RESPONSE=$(curl -s $HEALTH_URL || echo "{}")
    STATUS=$(echo $RESPONSE | jq -r '.status' 2>/dev/null || echo "unknown")
    
    if [ "$STATUS" = "healthy" ]; then
        print_status "✅ API is healthy"
    else
        print_error "❌ API health check failed: $RESPONSE"
        return 1
    fi
    
    # Check Docker containers
    print_status "Docker container status:"
    docker-compose ps
    
    # Check Celery workers
    docker exec vectordb-worker celery -A app.celery_config inspect active || true
}

# Function to setup Nginx
setup_nginx() {
    print_status "Configuring Nginx..."
    
    # Create Nginx configuration
    sudo tee /etc/nginx/sites-available/vectordb-api <<EOF
server {
    listen 80;
    server_name _;
    
    client_max_body_size 100M;
    
    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_read_timeout 300s;
        proxy_connect_timeout 75s;
    }
    
    location /flower/ {
        proxy_pass http://localhost:5555/;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
    }
}
EOF
    
    # Enable site
    sudo ln -sf /etc/nginx/sites-available/vectordb-api /etc/nginx/sites-enabled/
    sudo rm -f /etc/nginx/sites-enabled/default
    
    # Test and reload Nginx
    sudo nginx -t
    sudo systemctl reload nginx
}

# Function to show deployment info
show_info() {
    print_status "==================================================="
    print_status "Deployment Complete!"
    print_status "==================================================="
    print_status "API URL: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):8000"
    print_status "Flower URL: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):5555"
    print_status "Logs: $APP_DIR/logs/"
    print_status "==================================================="
    print_status "Useful commands:"
    print_status "  View logs: docker-compose logs -f"
    print_status "  Restart services: docker-compose restart"
    print_status "  Check status: docker-compose ps"
    print_status "==================================================="
}

# Main execution
main() {
    print_status "==================================================="
    print_status "Starting deployment process for $APP_NAME"
    print_status "==================================================="
    
    # Check if running on EC2
    if ! curl -s http://169.254.169.254/latest/meta-data/ > /dev/null 2>&1; then
        print_warning "This script should be run on an EC2 instance"
    fi
    
    # Install dependencies if needed
    if ! command_exists docker; then
        install_dependencies
    fi
    
    # Deploy application
    deploy_application
    
    # Setup CloudWatch
    setup_cloudwatch
    
    # Setup Nginx
    setup_nginx
    
    # Show deployment info
    show_info
    
    print_status "Deployment completed successfully!"
}

# Run main function
main "$@"