# Deployment Guide for VectorDB Catchup Lambda

## Important: Skip Container deploys

Docker is not installed. Don't even bother with it.

## Important: Add length to timeout

When you do sam deploy, make sure to increase the timeout because default is not enough.

## Important: Build and Deploy Timeouts

**CRITICAL:** Both `sam build` and `sam deploy` commands can timeout with default settings:
- `sam build` may timeout after 2 minutes when building multiple Lambda functions
- Always use extended timeout (10 minutes) for both commands
- S3 upload connection errors may occur during deploy - simply retry the deployment

## Important: Forcing Lambda Code Updates

When deploying updates to AWS Lambda functions, SAM may skip uploading if it detects the file hash hasn't changed. This can happen even when code has been modified. To ensure your changes are deployed:

## CRITICAL: Complete Force Deploy Process

### The following 3 steps MUST be executed in order to guarantee fresh deployment:

### 1. Clean Build Artifacts (REQUIRED - Never Skip)
```bash
rm -rf .aws-sam/
```
**Why:** Removes cached build artifacts that may prevent new code from being packaged.

### 2. Rebuild the Application (REQUIRED)
```bash
sam build
# Note: Do NOT use --use-container since Docker is not installed
```
**Why:** Creates fresh build artifacts from your current source code.

### 3. Deploy with Force Upload (REQUIRED - Use These Exact Flags)
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
**Why Critical Flags:**
- `--force-upload`: Forces upload even if SAM thinks files haven't changed
- `--s3-prefix "deploy-$(date +%s)"`: Uses unique timestamp to bypass S3 caching
- `--resolve-s3`: Auto-resolves S3 bucket (do NOT use with --s3-bucket flag)
- All three flags MUST be used together for guaranteed fresh deployment

**Note:** If S3 upload fails with connection errors, simply retry the same command - the deployment will resume

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

### Problem: SAM build timeout errors
**Solution:** Build can timeout with 11+ Lambda functions. Use extended timeout:
```bash
# If using AWS CLI directly, configure timeout
# Or simply retry - the build usually completes on second attempt
sam build
```

### Problem: S3 upload connection errors during deploy
**Symptom:** Error: "Connection was closed before we received a valid response"
**Solution:** This is often a transient network issue. Simply retry the exact same deploy command - it will resume from where it left off

### Problem: Cannot use both --resolve-s3 and --s3-bucket
**Solution:** Use ONLY `--resolve-s3` flag, never specify `--s3-bucket` in non-guided deployments

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
curl -X GET 'https://mj2cnwefn9.execute-api.us-east-1.amazonaws.com/dev/process?s3_key=253.csv&question_id=253&data_source=s3_csv&use_step_functions=true' \
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

## Lessons Learned (2025-09-04 Deployment)

### What Worked:
1. **The 3-step process is reliable:** Clean artifacts → Build → Deploy with force flags
2. **Unique S3 prefix with timestamp:** Critical for bypassing caching issues
3. **Using --resolve-s3:** Let SAM auto-resolve the bucket instead of specifying one
4. **Retry on failures:** S3 connection errors are often transient - retrying works

### What Didn't Work:
1. **Default timeouts:** Both build and deploy need extended timeouts for 11 Lambda functions
2. **Mixing --resolve-s3 and --s3-bucket:** These flags are mutually exclusive
3. **First S3 upload attempt:** Often fails with connection errors but succeeds on retry

### Recommended Approach:
Always follow the exact 3-step process in order, be patient with timeouts, and retry on S3 errors. The deployment will eventually succeed with persistence.