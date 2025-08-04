#!/bin/bash

# Deployment script for Vectordb Catchup Lambda function

set -e

# Configuration
STACK_NAME="vectordb-catchup"
ENVIRONMENT="${1:-dev}"
REGION="${AWS_REGION:-us-east-1}"
TEMPLATE_FILE="template.yaml"
PARAMETERS_FILE="parameters-${ENVIRONMENT}.json"

echo "=========================================="
echo "Deploying Vectordb Catchup Lambda Function"
echo "=========================================="
echo "Environment: $ENVIRONMENT"
echo "Region: $REGION"
echo "Stack Name: $STACK_NAME-$ENVIRONMENT"
echo "=========================================="

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed"
    exit 1
fi

# Check if SAM CLI is installed
if ! command -v sam &> /dev/null; then
    echo "Error: SAM CLI is not installed"
    exit 1
fi

# Check if parameters file exists
if [ ! -f "$PARAMETERS_FILE" ]; then
    echo "Error: Parameters file $PARAMETERS_FILE not found"
    echo "Please create the parameters file with your configuration"
    exit 1
fi

# Build the application
echo "Building SAM application..."
sam build --use-container

# Deploy the application
echo "Deploying SAM application..."
sam deploy \
    --stack-name "$STACK_NAME-$ENVIRONMENT" \
    --parameter-overrides file://"$PARAMETERS_FILE" \
    --capabilities CAPABILITY_IAM \
    --region "$REGION" \
    --confirm-changeset \
    --tags Environment="$ENVIRONMENT" Application="VectordbCatchup"

echo "=========================================="
echo "Deployment completed successfully!"
echo "=========================================="

# Get the API endpoint
echo "Getting API endpoint..."
API_ENDPOINT=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME-$ENVIRONMENT" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`ProcessEndpoint`].OutputValue' \
    --output text)

echo "API Endpoint: $API_ENDPOINT"
echo "=========================================="
echo "Example usage:"
echo "curl -X GET '$API_ENDPOINT?start_date=2025-01-01' -H 'x-api-key: YOUR_API_KEY'"
echo "=========================================="