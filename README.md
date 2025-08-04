# Vectordb Catchup Lambda Function

A serverless AWS Lambda function that processes Metabase data, vectorizes it using OpenAI embeddings, and stores the results in Pinecone for vector similarity search.

## Features

- **Metabase Integration**: Fetches data from Metabase questions with date parameters
- **Text Processing**: Sanitizes formatted text data for vectorization
- **OpenAI Embeddings**: Generates embeddings using text-embedding-3-small model
- **Pinecone Storage**: Stores vectors with metadata in Pinecone index
- **Task Management**: Tracks processing status using DynamoDB
- **Rate Limiting**: Prevents OpenAI API rate limit issues
- **Public API**: Secure endpoint with API key authentication

## Architecture

```
API Gateway → Lambda Function → [Metabase, OpenAI, Pinecone, DynamoDB]
```

### Components

- **API Gateway**: Public endpoint with x-api-key authentication
- **Lambda Function**: Main processing logic (15-minute timeout)
- **DynamoDB**: Task status tracking and queuing
- **CloudWatch**: Logging and monitoring
- **CloudFormation**: Infrastructure as Code

## API Endpoints

### Process Data
- **URL**: `GET /process`
- **Parameters**: `start_date` (required, format: YYYY-MM-DD)
- **Headers**: `x-api-key` (required)
- **Response**: Task information and processing status

### Check Task Status
- **URL**: `GET /status`
- **Parameters**: `task_id` (required)
- **Headers**: `x-api-key` (required)
- **Response**: Task status and progress

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `METABASE_URL` | Metabase base URL | `https://cbmetabase.com` |
| `METABASE_API_KEY` | Metabase API key | `mb_xxx...` |
| `METABASE_QUESTION_ID` | Question ID to query | `232` |
| `API_KEY` | API key for authentication | `abcdABCD1234!@` |
| `PINECONE_API_KEY` | Pinecone API key | `pcsk_xxx...` |
| `PINECONE_INDEX_NAME` | Pinecone index name | `clickbidtest` |
| `OPENAI_API_KEY` | OpenAI API key | `sk-proj-xxx...` |
| `DYNAMODB_TABLE_NAME` | DynamoDB table name | `vectordb-tasks-dev` |

## Deployment

### Prerequisites

1. **AWS CLI**: Installed and configured
2. **SAM CLI**: Installed for serverless deployment
3. **Docker**: Required for building dependencies

### Steps

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd vectordb-catchup-v2
   ```

2. **Configure parameters**
   ```bash
   # Edit parameters file for your environment
   cp parameters-dev.json parameters-prod.json
   # Update API keys and configuration
   ```

3. **Deploy**
   ```bash
   # Deploy to development
   ./deploy.sh dev
   
   # Deploy to production
   ./deploy.sh prod
   ```

### Manual Deployment

```bash
# Build
sam build --use-container

# Deploy
sam deploy --stack-name vectordb-catchup-dev \
  --parameter-overrides file://parameters-dev.json \
  --capabilities CAPABILITY_IAM \
  --region us-east-1
```

## Usage Examples

### Process Data
```bash
curl -X GET 'https://api-id.execute-api.us-east-1.amazonaws.com/dev/process?start_date=2025-01-01' \
  -H 'x-api-key: XXXXX'
```

### Check Task Status
```bash
curl -X GET 'https://api-id.execute-api.us-east-1.amazonaws.com/dev/status?task_id=uuid-here' \
  -H 'x-api-key: XXXXX'
```

## Processing Flow

1. **Input Validation**: Validates start_date parameter and API key
2. **Date Calculation**: Calculates end_date as start_date + 2 days
3. **Task Creation**: Creates task record in DynamoDB
4. **Data Retrieval**: Fetches data from Metabase using date parameters
5. **Text Processing**: Sanitizes formatted_text from each record
6. **Vectorization**: Generates embeddings using OpenAI (rate limited)
7. **Storage**: Upserts vectors to Pinecone with metadata
8. **Status Update**: Updates task status and progress in DynamoDB

## Data Flow

```
Metabase Query → Text Sanitization → OpenAI Embedding → Pinecone Storage
                                                      ↓
                                              DynamoDB Tracking
```

## Error Handling

- **API Errors**: Proper HTTP status codes and error messages
- **Rate Limiting**: OpenAI API calls are rate limited to prevent timeouts
- **Task Tracking**: Failed records are counted and logged
- **Retry Logic**: Built-in retry for transient failures
- **Monitoring**: CloudWatch alarms for errors and duration

## Monitoring

### CloudWatch Metrics
- Lambda execution duration
- Error rates
- DynamoDB read/write metrics
- API Gateway request metrics

### CloudWatch Alarms
- Processing function errors (>5 errors in 10 minutes)
- Processing duration (>14 minutes)

## Security

- **API Key Authentication**: Required for all endpoints
- **IAM Roles**: Least privilege access for Lambda functions
- **Parameter Store**: Secure storage for sensitive configuration
- **VPC**: Optional VPC deployment for enhanced security

## Performance Considerations

- **Timeout**: 15-minute Lambda timeout for processing up to 1000 records
- **Memory**: 1024MB memory allocation for optimal performance
- **Concurrency**: Limited to 5 concurrent executions
- **Rate Limiting**: 10 requests/second to OpenAI API
- **Batch Processing**: Pinecone upserts in batches of 100

## Troubleshooting

### Common Issues

1. **Timeout Errors**: Check CloudWatch logs for processing bottlenecks
2. **Rate Limit Errors**: Reduce OpenAI rate limit in configuration
3. **Memory Errors**: Increase Lambda memory allocation
4. **API Key Errors**: Verify API keys in parameter configuration

### Debugging

```bash
# Check Lambda logs
aws logs tail /aws/lambda/vectordb-processing-dev --follow

# Check DynamoDB tasks
aws dynamodb scan --table-name vectordb-tasks-dev

# Check API Gateway logs
aws logs tail /aws/apigateway/vectordb-api-dev --follow
```

## Development

### Local Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export METABASE_URL=https://cbmetabase.com
export METABASE_API_KEY=your-key
# ... other variables

# Run locally
python lambda_function.py
```

### Testing

```bash
# Unit tests
python -m pytest tests/

# Integration tests
python -m pytest tests/integration/
```

## License

This project is licensed under the MIT License.