# Docker Setup for EC2 VectorDB API

## Quick Start

The EC2 VectorDB API is fully containerized with Docker for easy deployment and testing.

### Prerequisites
- Docker Desktop installed and running
- `.env.local` file with your API keys (OpenAI, Pinecone, etc.)

### 1. Build and Start All Services

```bash
# Quick start - build and run everything
./docker-run.sh

# OR manually:
docker-compose build
docker-compose up -d
```

### 2. Available Services

After starting, the following services will be available:

| Service | URL | Description |
|---------|-----|-------------|
| API | http://localhost:8000 | Main FastAPI application |
| API Docs | http://localhost:8000/api/v1/docs | Interactive API documentation |
| Flower | http://localhost:5555 | Celery task monitoring |
| Redis | localhost:6379 | Task broker (internal) |

### 3. Test the API

```bash
# Check health
curl http://localhost:8000/health

# View API documentation
open http://localhost:8000/api/v1/docs

# Monitor background tasks
open http://localhost:5555
```

## Docker Commands

### Using docker-run.sh Script

```bash
# Build images
./docker-run.sh build

# Start services
./docker-run.sh up

# Stop services
./docker-run.sh down

# View logs
./docker-run.sh logs

# Check status
./docker-run.sh status

# Run tests
./docker-run.sh test

# Clean up everything
./docker-run.sh clean
```

### Manual Docker Commands

```bash
# Build all images
docker-compose build

# Start in background
docker-compose up -d

# View logs
docker-compose logs -f

# Stop all services
docker-compose down

# Rebuild specific service
docker-compose build api
docker-compose up -d api

# Enter container shell
docker exec -it ec2-vectordb-api-api-1 /bin/bash
```

## Configuration

### Environment Variables

The Docker setup uses `.env.local` for configuration. Key variables:

```bash
# API Keys (required)
OPENAI_API_KEY=sk-proj-xxx
PINECONE_API_KEY=pcsk_xxx
API_KEY=your-api-key

# Local file processing
USE_LOCAL_FILES=true
LOCAL_CSV_FOLDER=/app/test_data/csv_files

# Redis (internal Docker network)
REDIS_HOST=redis
REDIS_PORT=6379
```

### Volumes

The following volumes are mounted for development:

- `./test_data:/app/test_data` - Test CSV files
- `./app:/app/app` - Live code reload during development

## Testing with Docker

### Process a Local CSV File

1. Place your CSV file in `test_data/csv_files/`
2. Call the process endpoint:

```bash
# Get API key from .env.local
API_KEY=$(grep "^API_KEY=" .env.local | cut -d'=' -f2)

# Submit processing task
curl -X POST http://localhost:8000/api/v1/process \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "data_source": "s3_csv",
    "s3_key": "sample.csv",
    "batch_size": 1000,
    "namespace": "v2"
  }'

# Check task status (use returned task_id)
curl http://localhost:8000/api/v1/status/{task_id} \
  -H "X-API-Key: $API_KEY"
```

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker-compose logs api
docker-compose logs worker

# Verify Docker is running
docker info

# Check port conflicts
lsof -i :8000
lsof -i :6379
lsof -i :5555
```

### Worker/Flower Exits

Common issues:
- Missing dependencies - rebuild with `docker-compose build`
- Redis connection - ensure Redis container is healthy
- Import errors - check Python package compatibility

### Memory Issues

```bash
# Check Docker resources
docker system df

# Clean up unused resources
docker system prune -a
```

### Debugging

```bash
# Enter container for debugging
docker exec -it ec2-vectordb-api-api-1 /bin/bash

# Check Python packages
docker exec ec2-vectordb-api-api-1 pip list

# Test imports
docker exec ec2-vectordb-api-api-1 python -c "from app.main import app"
```

## Production Deployment

For production deployment on EC2:

1. **Update Configuration**
   - Use environment variables instead of .env files
   - Configure proper API keys and secrets
   - Set `ENVIRONMENT=production`

2. **Security**
   - Use HTTPS with proper SSL certificates
   - Implement rate limiting
   - Set up proper firewall rules

3. **Scaling**
   - Use Docker Swarm or Kubernetes for orchestration
   - Configure multiple worker instances
   - Set up load balancing

4. **Monitoring**
   - Configure CloudWatch or Prometheus
   - Set up alerts for errors and performance
   - Monitor resource usage

## Architecture

```
┌─────────────────────────────────────────────┐
│              Docker Network                  │
│                                              │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐     │
│  │  Redis  │◄─┤   API   │  │ Flower  │     │
│  └─────────┘  └─────────┘  └─────────┘     │
│       ▲            │             │          │
│       │            │             │          │
│       └────────────┼─────────────┘          │
│                    │                        │
│              ┌─────────┐                    │
│              │ Worker  │                    │
│              └─────────┘                    │
└─────────────────────────────────────────────┘
        │                 │
        ▼                 ▼
   [OpenAI API]      [Pinecone]
```

## Files

- `Dockerfile` - Container definition
- `docker-compose.yml` - Multi-container orchestration
- `requirements-docker.txt` - Python dependencies
- `.dockerignore` - Files to exclude from build
- `docker-run.sh` - Convenience script for Docker operations

## Support

For issues or questions:
1. Check container logs with `docker-compose logs`
2. Verify all API keys in `.env.local`
3. Ensure Docker Desktop has sufficient resources allocated
4. Review the main README for API documentation