# VectorDB Catchup Lambda Function - Claude Code Guide

## Project Overview
This is a serverless AWS Lambda application that processes data from Metabase or S3 CSV files, generates vector embeddings using OpenAI, and stores them in Pinecone for vector similarity search. The system is designed for high-volume data processing with robust error handling and task tracking.

## Architecture

### Core Components
- **API Gateway**: Public REST API with x-api-key authentication
- **Lambda Functions**: Multiple functions for different processing stages
- **Step Functions**: State machine for large CSV processing with chunking
- **DynamoDB**: Task status tracking and distributed locking
- **CloudWatch**: Logging, monitoring, and alerting

### Data Flow
```
API Gateway → Main Lambda → Async Lambda/Step Functions → [Metabase/S3] → OpenAI → Pinecone
                    ↓                           ↓
                DynamoDB (Task Tracking) ←──────┘
```

## Key Features
- **Dual Data Sources**: Supports both Metabase queries and S3 CSV files
- **Async Processing**: Non-blocking API with task status tracking
- **Large File Support**: Step Functions workflow handles 100K+ record CSVs
- **Rate Limiting**: Configurable rate limits for OpenAI API calls
- **Batch Processing**: Pinecone upserts in configurable batch sizes
- **Namespace Support**: Proper handling of Pinecone namespaces for data organization
- **Comprehensive Monitoring**: CloudWatch alarms for errors and duration

## Project Structure
```
vectordb-catchup-v2/
├── lambda_function.py       # Main entry point (sync handler)
├── main_handler.py          # Main processing logic
├── async_handler.py         # Async processing handler
├── task_status.py          # Status checking handler
├── services/               # External service clients
│   ├── dynamodb_client.py # DynamoDB operations
│   ├── metabase_client.py # Metabase API integration
│   ├── openai_client.py   # OpenAI embeddings
│   ├── pinecone_client.py # Pinecone vector storage
│   └── s3_csv_client.py   # S3 CSV reading
├── step_functions/         # Step Functions handlers
│   ├── csv_initializer.py # CSV processing initialization
│   ├── chunk_processor.py # Process CSV chunks
│   └── result_aggregator.py # Aggregate chunk results
├── utils/                  # Utility functions
│   ├── date_utils.py      # Date handling
│   └── text_sanitizer.py  # Text preprocessing
├── tests/                  # Comprehensive test suite
│   ├── unit/              # Unit tests for services
│   ├── integration/       # Integration tests for handlers
│   ├── e2e/              # End-to-end workflow tests
│   ├── fixtures/         # Test data and mocks
│   └── conftest.py       # Pytest configuration
├── .github/workflows/      # CI/CD pipelines
│   ├── ci-cd.yml         # Main CI/CD workflow
│   └── test-pr.yml       # PR validation workflow
├── template.yaml          # SAM/CloudFormation template
├── deploy.sh              # Deployment script
├── pytest.ini             # Pytest configuration
├── .coveragerc           # Coverage configuration
├── requirements-test.txt  # Testing dependencies
└── parameters-*.json      # Environment configurations
```

## API Endpoints

### Process Data
```bash
GET /process?start_date=YYYY-MM-DD[&question_id=123][&data_source=metabase|s3_csv][&s3_key=path/to/file.csv]
Headers: x-api-key: YOUR_API_KEY
```

### Check Status
```bash
GET /status?task_id=UUID
Headers: x-api-key: YOUR_API_KEY
```

## Environment Variables
- `METABASE_URL`: Metabase instance URL
- `METABASE_API_KEY`: Metabase authentication
- `METABASE_QUESTION_ID`: Default question to query
- `OPENAI_API_KEY`: OpenAI API key
- `PINECONE_API_KEY`: Pinecone API key
- `PINECONE_INDEX_NAME`: Target Pinecone index
- `PINECONE_NAMESPACE`: Namespace for vector organization (default: "v2")
- `DYNAMODB_TABLE_NAME`: Task tracking table
- `S3_BUCKET_NAME`: Bucket for CSV files
- `API_KEY`: API Gateway authentication key

## Development Commands

### Deploy to Environment
- ALWAYS use DEPLOY_README.md for instructions on deployment.
```bash
./deploy.sh dev    # Deploy to dev environment
./deploy.sh prod   # Deploy to production
```

### Manual SAM Commands
```bash
sam build --use-container
sam deploy --stack-name vectordb-catchup-dev \
  --parameter-overrides file://parameters-dev.json \
  --capabilities CAPABILITY_IAM
```

### Testing Commands
```bash
# Run all tests
pytest

# Run unit tests only
pytest tests/unit -v

# Run integration tests
pytest tests/integration -v

# Run with coverage report
pytest --cov=. --cov-report=html

# Run specific test file
pytest tests/unit/test_openai_client.py -v

# Run tests in parallel
pytest -n auto
```

### Monitoring
```bash
# Check Lambda logs
aws logs tail /aws/lambda/vectordb-processing-dev --follow

# Monitor task status
aws dynamodb scan --table-name vectordb-tasks-dev

# Test the API
curl -X GET 'https://API_ID.execute-api.us-east-1.amazonaws.com/dev/process?start_date=2025-01-01' \
  -H 'x-api-key: YOUR_KEY'
```

## Processing Workflows

### Metabase Data Processing
1. Validate API key and parameters
2. Create task in DynamoDB
3. Trigger async Lambda
4. Fetch data from Metabase with date range
5. Sanitize text content
6. Generate OpenAI embeddings (rate limited)
7. Batch upsert to Pinecone
8. Update task status

### S3 CSV Processing (Large Files)
1. Validate parameters and S3 key
2. Create task in DynamoDB with chunk tracking
3. Trigger Step Functions state machine
4. Initialize: Count rows, create chunks, initialize chunk statuses
5. Process chunks in parallel (max 5 concurrent)
6. Each chunk: Read CSV, generate embeddings, upsert with namespace
7. Update individual chunk status in DynamoDB
8. Aggregate results from all chunks
9. Update final task status with totals

## Key Technical Details

### Rate Limiting
- OpenAI API calls limited to 20 requests/second
- Configurable in `OpenAIClient` initialization
- Prevents API throttling and Lambda timeouts

### Batch Processing
- Pinecone upserts in batches of 100 vectors
- Reduces API calls and improves performance
- Handles large datasets efficiently

### Error Handling
- Comprehensive try-catch blocks
- Task status tracking for failures
- CloudWatch alarms for monitoring
- Retry logic in Step Functions

### Lambda Configuration
- **Timeout**: 15 minutes (900 seconds)
- **Memory**: 1024 MB
- **Concurrency**: Limited to 5 for Step Functions
- **Runtime**: Python 3.9

## Common Issues & Solutions

### Timeout Errors
- Check CloudWatch logs for bottlenecks
- Reduce batch sizes or rate limits
- Consider using Step Functions for large datasets

### Rate Limit Errors
- Decrease `rate_limit_per_second` in OpenAIClient
- Implement exponential backoff
- Monitor OpenAI API usage

### Memory Errors
- Increase Lambda memory allocation
- Process data in smaller chunks
- Optimize data structures

## Security Considerations
- API key authentication required
- Sensitive parameters stored with NoEcho
- IAM roles with least privilege
- Parameter Store for secrets (optional)
- VPC deployment supported

## Testing Infrastructure

### Test Coverage
The project includes a comprehensive test suite with 80% minimum coverage requirement:

1. **Unit Tests** (`tests/unit/`)
   - Complete coverage for all service clients (DynamoDB, OpenAI, Pinecone, Metabase, S3)
   - Tests for utility functions and error handling
   - Mocked external dependencies

2. **Integration Tests** (`tests/integration/`)
   - Lambda handler testing with mocked AWS services
   - API Gateway event simulation
   - Step Functions workflow testing

3. **End-to-End Tests** (`tests/e2e/`)
   - Complete workflow validation
   - Error propagation testing
   - Performance benchmarking

### CI/CD Pipeline
- **Automated Testing**: Runs on every push and PR
- **Security Scanning**: Bandit and Safety checks
- **Code Quality**: Black formatting, Flake8 linting, MyPy type checking
- **Deployment Automation**: Separate dev/prod deployments
- **Coverage Reporting**: Codecov integration with HTML reports

## Recent Updates (January 2025)
- **Fixed Namespace Handling**: Corrected Pinecone namespace configuration across all handlers
- **Improved DynamoDB Updates**: Fixed chunk status tracking with proper attribute updates
- **Enhanced Error Handling**: Better error reporting for chunk processing failures
- **Optimized OpenAI Client**: Improved rate limiting and batch processing
- **Comprehensive Testing Suite**: Added full test coverage with unit, integration, and e2e tests
- **CI/CD Pipeline**: Implemented GitHub Actions workflows for automated testing and deployment
- **Test Fixtures**: Created sample data and mock responses for consistent testing
- **Coverage Requirements**: Enforced 80% minimum code coverage with detailed reporting

## Future Improvements
- Implement retry queue for failed records
- Add data validation schemas
- Support for incremental updates
- Metrics dashboard in CloudWatch
- Support for multiple embedding models
- Caching layer for frequently accessed data

## Important Notes
- ALWAYS use DEPLOY_README.md for instructions on deployment.
- Always validate API keys before deployment
- Monitor CloudWatch alarms for production issues
- Keep OpenAI rate limits conservative to avoid timeouts
- Use Step Functions for files > 10K records
- Regular DynamoDB cleanup for old task records
- Ensure Pinecone index dimensions match embedding model

## Quick Troubleshooting

### Lambda Not Processing
1. Check CloudWatch logs for errors
2. Verify environment variables
3. Confirm IAM permissions
4. Test API keys independently

### Slow Processing
1. Monitor OpenAI rate limiting
2. Check Pinecone batch sizes
3. Verify Lambda memory allocation
4. Consider parallel processing

### Data Not in Pinecone
1. Check task status in DynamoDB
2. Verify embedding generation
3. Confirm Pinecone connectivity
4. Review metadata formatting

## Contact & Support
For issues or questions about this Lambda function:
1. Check CloudWatch logs first
2. Review DynamoDB task status
3. Verify all API keys are valid
4. Ensure proper IAM permissions