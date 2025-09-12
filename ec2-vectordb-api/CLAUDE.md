# EC2 VectorDB Processing API - Claude Code Guide

## Project Overview
A standalone Python API application designed to run on EC2 instances, replacing the Lambda-based architecture to handle large-scale CSV processing (100,000+ rows) without timeout or size limitations. This system processes data from Metabase or S3, generates vector embeddings using OpenAI, and stores them in Pinecone.

## Why EC2 Instead of Lambda?
- **No Time Limits**: Lambda has 15-minute timeout; EC2 can process for hours/days
- **No Payload Limits**: Lambda has 6MB sync/256KB async limits; EC2 handles GBs
- **Better Resource Control**: Full CPU/memory of EC2 instance available
- **Cost Efficiency**: For large batch processing, EC2 is more economical
- **Easier Debugging**: Direct SSH access, live logs, and profiling tools

## Architecture

### Core Components
```
Nginx (Reverse Proxy) → FastAPI Application → Background Workers (Celery)
                              ↓                          ↓
                        DynamoDB/Redis ←─────────────────┘
                              ↓
                    [Metabase, S3, OpenAI, Pinecone]
```

### Technology Stack
- **Web Framework**: FastAPI (async Python web framework)
- **Task Queue**: Celery with Redis broker
- **Process Management**: Supervisor/systemd
- **Reverse Proxy**: Nginx with rate limiting
- **Database**: DynamoDB for task tracking (or PostgreSQL/Redis)
- **Monitoring**: CloudWatch integration (optional)

## Project Structure
```
ec2-vectordb-api/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI application entry point
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes.py           # API endpoints
│   │   ├── auth.py             # API key authentication
│   │   └── models.py           # Pydantic models
│   ├── workers/
│   │   ├── __init__.py
│   │   ├── celery_app.py       # Celery configuration
│   │   ├── tasks.py            # Background tasks
│   │   └── chunk_processor.py  # Parallel chunk processing
│   ├── services/               # External service integrations
│   │   ├── __init__.py
│   │   ├── dynamodb_client.py # DynamoDB operations
│   │   ├── metabase_client.py # Metabase API client
│   │   ├── openai_client.py   # OpenAI embeddings
│   │   ├── pinecone_client.py # Pinecone vector storage
│   │   └── s3_csv_client.py   # S3 CSV streaming
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── date_utils.py      # Date handling utilities
│   │   ├── text_sanitizer.py  # Text preprocessing
│   │   └── chunking.py        # CSV chunking logic
│   └── config.py              # Configuration management
├── infrastructure/
│   ├── docker/
│   │   ├── Dockerfile         # Container definition
│   │   └── docker-compose.yml # Multi-container setup
│   ├── systemd/
│   │   ├── vectordb-api.service    # API service
│   │   └── vectordb-worker.service # Worker service
│   └── nginx/
│       └── vectordb-api.conf  # Nginx configuration
├── scripts/
│   ├── setup_ec2.sh          # EC2 initialization
│   ├── deploy.sh             # Deployment automation
│   └── health_check.sh       # Health monitoring
├── tests/
│   ├── __init__.py
│   ├── test_api.py
│   └── test_workers.py
├── requirements.txt
├── .env.example
├── README.md
└── CLAUDE.md (this file)
```

## API Endpoints

### Process Data
```http
POST /api/v1/process
Content-Type: application/json
X-API-Key: your-api-key

{
  "data_source": "s3_csv|metabase",
  "s3_key": "path/to/file.csv",  // if data_source is s3_csv
  "question_id": 123,             // if data_source is metabase
  "start_date": "2025-01-01",
  "date_column": "created_at",
  "batch_size": 1000,
  "namespace": "v2"
}

Response:
{
  "task_id": "uuid-here",
  "status": "pending",
  "message": "Task queued for processing"
}
```

### Check Task Status
```http
GET /api/v1/status/{task_id}
X-API-Key: your-api-key

Response:
{
  "task_id": "uuid-here",
  "status": "processing|completed|failed",
  "progress": {
    "total_records": 100000,
    "processed_records": 50000,
    "failed_records": 10,
    "chunks_completed": 50,
    "chunks_total": 100
  },
  "started_at": "2025-01-01T00:00:00Z",
  "completed_at": "2025-01-01T01:00:00Z",
  "error": null
}
```

### Health Check
```http
GET /api/v1/health

Response:
{
  "status": "healthy",
  "version": "1.0.0",
  "workers": {
    "active": 4,
    "available": 4
  },
  "redis": "connected",
  "dynamodb": "connected"
}
```

## Processing Architecture

### Large CSV Processing Flow
1. **File Analysis**: Determine total rows and memory requirements
2. **Chunking Strategy**: Divide into optimal chunks based on available resources
3. **Parallel Processing**: 
   - Use multiprocessing.Pool for CPU-bound embedding generation
   - Process chunks concurrently with configurable worker count
4. **Streaming**: Read CSV in chunks to avoid memory overflow
5. **Progress Tracking**: Update task status per chunk completion
6. **Error Recovery**: Failed chunks can be retried independently

### Rate Limiting Strategy
- **OpenAI**: Token bucket algorithm with 20 req/s default
- **Pinecone**: Batch uploads with exponential backoff
- **Metabase**: Respect API rate limits with retry logic

## Deployment

### EC2 Instance Setup
```bash
# Recommended Instance Types
- Development: t3.large (2 vCPUs, 8GB RAM)
- Production: m5.xlarge (4 vCPUs, 16GB RAM)
- High Volume: m5.2xlarge (8 vCPUs, 32GB RAM)

# Storage
- Root: 30GB GP3
- Data: 100GB+ GP3 (for temporary file storage)
```

### Quick Start
```bash
# 1. Clone repository
git clone <repo-url>
cd ec2-vectordb-api

# 2. Install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env with your credentials

# 4. Run application
# Development
uvicorn app.main:app --reload

# Production
gunicorn app.main:app -w 4 -k uvicorn.workers.UvicornWorker
```

### Systemd Service Setup
```bash
# Copy service files
sudo cp infrastructure/systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload

# Start services
sudo systemctl start vectordb-api
sudo systemctl start vectordb-worker

# Enable on boot
sudo systemctl enable vectordb-api
sudo systemctl enable vectordb-worker
```

## Monitoring

### Key Metrics
- **API Response Time**: Target < 200ms for status checks
- **Task Processing Time**: Track per 1000 records
- **Memory Usage**: Monitor for leaks during large file processing
- **Worker Queue Length**: Alert if > 100 pending tasks
- **Error Rate**: Alert if > 1% of chunks fail

### Health Checks
```bash
# API health
curl http://localhost:8000/api/v1/health

# Worker health
celery -A app.workers.celery_app inspect active

# System resources
htop
df -h
free -m
```

## Troubleshooting

### Common Issues

#### High Memory Usage
- Reduce CHUNK_SIZE in configuration
- Increase swap space on EC2 instance
- Use streaming for CSV reading

#### Slow Processing
- Increase MAX_WORKERS for parallel processing
- Optimize OpenAI rate limits
- Use larger EC2 instance

#### Local Testing

1. **Configuration**: Two environment files are provided:
   - `.env.example` - Template with all configuration options
   - `.env.local` - Pre-configured for local testing with real API keys

2. **Local File Mode**: 
   - Set `USE_LOCAL_FILES=true` to enable local filesystem mode
   - Place CSV files in `./test_data/csv_files/` directory
   - Sample file provided: `sample.csv` with 10 test records
   - System automatically reads from local folder instead of S3

3. **Test Data Structure**:
   ```
   test_data/
   ├── csv_files/        # Place your CSV test files here
   │   ├── sample.csv    # Example small CSV file
   │   ├── 253.csv       # Your actual test files
   │   └── ...
   └── README.md         # Usage instructions
   ```

4. **Using Local Files**:
   ```bash
   # Copy local configuration
   cp .env.local .env
   
   # Place CSV files in test_data/csv_files/
   # Reference them by filename when calling API
   ```

**Simple Test Server:**
Due to Python 3.13 compatibility issues with pydantic/FastAPI, a simplified test server was created:

**Test Server Details:**
- File: `simple_server.py` (uses only Python standard library)
- Port: 8001 (changed from 8000 due to port conflict)
- API Key: `62jtdXZX035M78uHYGvQu1TV6ZBMZkkh6mqoTzJE`

**To run the test server:**
```bash
cd ec2-vectordb-api
python3 simple_server.py
```

**Successfully Tested Endpoints:**
```bash
# 1. Health Check (no auth required)
curl http://localhost:8001/health

# 2. Submit Processing Task (using local CSV file)
curl -X POST http://localhost:8001/api/v1/process \
  -H "X-API-Key: 62jtdXZX035M78uHYGvQu1TV6ZBMZkkh6mqoTzJE" \
  -H "Content-Type: application/json" \
  -d '{"data_source": "s3_csv", "s3_key": "sample.csv", "batch_size": 1000}'

# 3. Check Task Status
curl http://localhost:8001/api/v1/status/{task_id} \
  -H "X-API-Key: 62jtdXZX035M78uHYGvQu1TV6ZBMZkkh6mqoTzJE"

# 4. List All Tasks
curl http://localhost:8001/api/v1/tasks \
  -H "X-API-Key: 62jtdXZX035M78uHYGvQu1TV6ZBMZkkh6mqoTzJE"
```

**For production FastAPI (requires Python ≤3.12):**
```bash
cd ec2-vectordb-api
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your API keys
uvicorn app.main:app --reload
```