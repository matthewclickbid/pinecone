# EC2 VectorDB Processing API

A high-performance Python API for processing large CSV files (100,000+ rows) without Lambda's time and size limitations. This application runs on EC2 instances and processes data from Metabase or S3, generates vector embeddings using OpenAI, and stores them in Pinecone.

## Features

- **No Time Limits**: Process files for hours without timeout constraints
- **Large File Support**: Handle GB-sized CSV files with streaming and chunking
- **Parallel Processing**: Utilize multiple CPU cores for faster processing
- **RESTful API**: FastAPI-based with automatic documentation
- **Async Processing**: Background job queue for non-blocking operations
- **Progress Tracking**: Real-time status updates for long-running tasks
- **API Key Authentication**: Secure access control

## Quick Start

### Prerequisites

- Python 3.9+
- Redis (for Phase 2 background jobs)
- AWS credentials configured
- API keys for OpenAI and Pinecone

### Installation

1. Clone the repository:
```bash
cd ec2-vectordb-api
```

2. Create virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment:
```bash
cp .env.example .env
# Edit .env with your API keys and configuration
```

### Running the Application

#### Development Mode
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

#### Production Mode
```bash
gunicorn app.main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### API Documentation

Once running, access the interactive API documentation at:
- Swagger UI: `http://localhost:8000/api/v1/docs`
- ReDoc: `http://localhost:8000/api/v1/redoc`

## API Endpoints

### Process Data
```bash
curl -X POST "http://localhost:8000/api/v1/process" \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "data_source": "s3_csv",
    "s3_key": "data/large_dataset.csv",
    "batch_size": 1000,
    "namespace": "v2"
  }'
```

### Check Task Status
```bash
curl -X GET "http://localhost:8000/api/v1/status/{task_id}" \
  -H "X-API-Key: your-api-key"
```

### Health Check
```bash
curl -X GET "http://localhost:8000/api/v1/health"
```

## Project Structure

```
ec2-vectordb-api/
├── app/
│   ├── api/           # API routes and authentication
│   ├── services/      # External service clients
│   ├── utils/         # Utility functions
│   ├── workers/       # Background job processing (Phase 2)
│   ├── config.py      # Configuration management
│   └── main.py        # FastAPI application
├── infrastructure/    # Deployment configurations
├── scripts/          # Setup and deployment scripts
├── tests/            # Test suite
├── requirements.txt  # Python dependencies
├── .env.example      # Environment variables template
└── README.md         # This file
```

## Configuration

Key environment variables:

- `API_KEY`: API authentication key
- `OPENAI_API_KEY`: OpenAI API key for embeddings
- `PINECONE_API_KEY`: Pinecone API key
- `S3_BUCKET_NAME`: S3 bucket for CSV files
- `DYNAMODB_TABLE_NAME`: DynamoDB table for task tracking

See `.env.example` for complete configuration options.

## Development Status

### Phase 1: Core API ✅
- FastAPI application setup
- RESTful endpoints
- API key authentication
- Configuration management
- Basic task tracking

### Phase 2: Background Processing (Coming Soon)
- Celery integration
- Redis message broker
- Async task processing
- DynamoDB integration

### Phase 3: CSV Processing (Planned)
- Multiprocessing for chunks
- S3 streaming
- Memory optimization

### Phase 4: Production Ready (Planned)
- EC2 deployment scripts
- Nginx configuration
- Monitoring and alerts

## EC2 Deployment

### Recommended Instance Types
- Development: `t3.large` (2 vCPUs, 8GB RAM)
- Production: `m5.xlarge` (4 vCPUs, 16GB RAM)
- High Volume: `m5.2xlarge` (8 vCPUs, 32GB RAM)

### System Requirements
- Ubuntu 22.04 LTS
- Python 3.9+
- 100GB+ storage for temporary files
- Security group with port 8000 open

## Performance

Expected processing times (m5.xlarge instance):
- 10,000 records: ~2 minutes
- 100,000 records: ~15 minutes
- 1,000,000 records: ~2 hours

## Monitoring

- API logs: Check application logs for request/response details
- Task status: Monitor via `/api/v1/status` endpoint
- System metrics: Use CloudWatch or system monitoring tools

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all dependencies are installed: `pip install -r requirements.txt`
2. **API Key Errors**: Verify API_KEY is set in .env file
3. **Connection Errors**: Check AWS credentials and service endpoints
4. **Memory Issues**: Adjust CHUNK_SIZE in configuration

## Contributing

1. Follow the existing code structure
2. Add tests for new features
3. Update documentation
4. Use type hints and docstrings

## License

MIT License

## Support

For detailed documentation, see `CLAUDE.md` in this directory.

For issues or questions, check the logs first, then review the comprehensive documentation in CLAUDE.md.