# Deployment Guide for VectorDB Catchup Lambda

## Important: Skip Container deploys

Docker is not installed. Don't even bother with it.

## Important: Add length to timeout

When you do sam deploy, make sure to increase the timeout because default is not enough.

## Important: Forcing Lambda Code Updates

When deploying updates to AWS Lambda functions, SAM may skip uploading if it detects the file hash hasn't changed. This can happen even when code has been modified. To ensure your changes are deployed:

## Deployment Steps

### 1. Clean Build Artifacts (Required for code changes)
```bash
rm -rf .aws-sam/
```

### 2. Rebuild the Application
```bash
sam build
# or with container if Docker is running:
# sam build --use-container
```

### 3. Deploy with Force Upload
```bash
sam deploy \
  --stack-name vectordb-catchup-dev \
  --s3-prefix "deploy-$(date +%s)" \
  --parameter-overrides "$(cat parameter_overrides.txt)" \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --region us-east-1 \
  --no-confirm-changeset \
  --resolve-s3 \
  --force-upload
```

## Key Deployment Flags

- `--force-upload`: Forces SAM to upload artifacts even if they appear unchanged
- `--s3-prefix "deploy-$(date +%s)"`: Uses timestamp to ensure unique S3 path
- `--no-confirm-changeset`: Automatically deploys without manual confirmation
- `--resolve-s3`: Automatically resolves S3 bucket for artifacts

## Environment-Specific Deployment

### Development
```bash
./deploy.sh dev
```

### Production
```bash
./deploy.sh prod
```

## Troubleshooting Deployment Issues

### Problem: "File with same data already exists... skipping upload"
This means Lambda code won't be updated. Solution:
1. Remove `.aws-sam/` directory
2. Rebuild with `sam build`
3. Deploy with `--force-upload` flag

### Problem: Changes not reflecting in Lambda
1. Check CloudWatch logs to confirm which version is running
2. Verify the CloudFormation stack shows UPDATE_COMPLETE
3. Force a new deployment with unique S3 prefix

### Problem: Docker container build fails
Use `sam build` without `--use-container` flag if Docker isn't available

## Verification After Deployment

1. Check CloudFormation stack status:
```bash
aws cloudformation describe-stacks \
  --stack-name vectordb-catchup-dev \
  --region us-east-1 \
  --query 'Stacks[0].StackStatus'
```

2. Verify Lambda function update time:
```bash
aws lambda get-function \
  --function-name vectordb-migration-handler-dev \
  --region us-east-1 \
  --query 'Configuration.LastModified'
```

3. Test the endpoints:
```bash
# Process endpoint
curl -X GET 'https://mj2cnwefn9.execute-api.us-east-1.amazonaws.com/dev/process?start_date=2025-01-01' \
  -H 'x-api-key: YOUR_API_KEY'

# Migration endpoint  
curl -X GET 'https://mj2cnwefn9.execute-api.us-east-1.amazonaws.com/dev/migrate-namespaces?dry_run=true' \
  -H 'x-api-key: YOUR_API_KEY'
```

## Quick Deploy Script

Create a `force-deploy.sh` script:
```bash
#!/bin/bash
set -e

echo "Cleaning build artifacts..."
rm -rf .aws-sam/

echo "Building application..."
sam build

echo "Deploying with force upload..."
sam deploy \
  --stack-name vectordb-catchup-dev \
  --s3-prefix "force-deploy-$(date +%s)" \
  --parameter-overrides "$(cat parameter_overrides.txt)" \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --region us-east-1 \
  --no-confirm-changeset \
  --resolve-s3 \
  --force-upload

echo "Deployment complete!"
```

Make it executable: `chmod +x force-deploy.sh`

## Important Notes

- Always use `--force-upload` when you need to ensure code changes are deployed
- Clean build artifacts when making significant code changes
- Monitor CloudWatch logs after deployment to verify the new code is running
- Use unique S3 prefixes to avoid caching issues