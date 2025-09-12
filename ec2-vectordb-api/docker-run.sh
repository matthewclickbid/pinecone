#!/bin/bash

# EC2 VectorDB API - Docker Quick Start Script
# This script builds and runs the containerized API with all services

set -e  # Exit on error

echo "🚀 EC2 VectorDB API - Docker Setup"
echo "=================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Check if .env.local exists
if [ ! -f ".env.local" ]; then
    echo "❌ .env.local file not found!"
    echo "Please ensure .env.local exists with your API keys."
    exit 1
fi

# Function to display usage
usage() {
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  build    - Build Docker images"
    echo "  up       - Start all services"
    echo "  down     - Stop all services"
    echo "  restart  - Restart all services"
    echo "  logs     - Show logs from all services"
    echo "  test     - Run API tests"
    echo "  clean    - Remove containers and volumes"
    echo "  status   - Check service status"
    echo ""
    echo "If no command is provided, it will build and start all services."
}

# Function to check service health
check_health() {
    echo ""
    echo "🔍 Checking service health..."
    
    # Check Redis
    if docker-compose exec -T redis redis-cli ping > /dev/null 2>&1; then
        echo "✅ Redis is healthy"
    else
        echo "⚠️  Redis is not responding"
    fi
    
    # Check API
    if curl -s http://localhost:8000/health > /dev/null 2>&1; then
        echo "✅ API is healthy"
        echo "   API Docs: http://localhost:8000/api/v1/docs"
    else
        echo "⚠️  API is not responding"
    fi
    
    # Check Flower
    if curl -s http://localhost:5555 > /dev/null 2>&1; then
        echo "✅ Flower (Celery monitor) is healthy"
        echo "   Monitor: http://localhost:5555"
    else
        echo "⚠️  Flower is not responding"
    fi
}

# Function to run tests
run_tests() {
    echo ""
    echo "🧪 Running API tests..."
    
    # Test health endpoint
    echo "Testing /health endpoint..."
    curl -s http://localhost:8000/health | python3 -m json.tool
    
    echo ""
    echo "Testing /api/v1/health endpoint..."
    curl -s http://localhost:8000/api/v1/health | python3 -m json.tool
    
    # Get API key from .env.local
    API_KEY=$(grep "^API_KEY=" .env.local | cut -d'=' -f2)
    
    if [ -n "$API_KEY" ]; then
        echo ""
        echo "Testing authenticated endpoint..."
        curl -s -H "X-API-Key: $API_KEY" http://localhost:8000/api/v1/tasks | python3 -m json.tool
    fi
}

# Main script logic
case "$1" in
    build)
        echo "🔨 Building Docker images..."
        docker-compose build
        echo "✅ Build complete!"
        ;;
    
    up)
        echo "🚀 Starting services..."
        docker-compose up -d
        sleep 5  # Wait for services to start
        check_health
        ;;
    
    down)
        echo "🛑 Stopping services..."
        docker-compose down
        echo "✅ Services stopped!"
        ;;
    
    restart)
        echo "🔄 Restarting services..."
        docker-compose restart
        sleep 5
        check_health
        ;;
    
    logs)
        echo "📋 Showing logs (Ctrl+C to exit)..."
        docker-compose logs -f
        ;;
    
    test)
        run_tests
        ;;
    
    clean)
        echo "🧹 Cleaning up Docker resources..."
        docker-compose down -v
        echo "✅ Cleanup complete!"
        ;;
    
    status)
        echo "📊 Service Status:"
        docker-compose ps
        check_health
        ;;
    
    -h|--help|help)
        usage
        ;;
    
    "")
        # Default: build and start
        echo "🔨 Building Docker images..."
        docker-compose build
        
        echo ""
        echo "🚀 Starting services..."
        docker-compose up -d
        
        echo ""
        echo "⏳ Waiting for services to be ready..."
        sleep 8
        
        check_health
        
        echo ""
        echo "✅ EC2 VectorDB API is ready!"
        echo ""
        echo "📚 Available endpoints:"
        echo "   - API:     http://localhost:8000"
        echo "   - Docs:    http://localhost:8000/api/v1/docs"
        echo "   - Flower:  http://localhost:5555"
        echo ""
        echo "📋 To view logs:    ./docker-run.sh logs"
        echo "🛑 To stop:         ./docker-run.sh down"
        echo "❓ For help:        ./docker-run.sh help"
        ;;
    
    *)
        echo "❌ Unknown command: $1"
        usage
        exit 1
        ;;
esac