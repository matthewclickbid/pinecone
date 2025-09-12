#!/bin/bash

# AWS Infrastructure Setup Script
# This script creates all necessary AWS resources for the VectorDB API

set -e

# Configuration
REGION="us-east-1"
STACK_NAME="vectordb-api-stack"
ENVIRONMENT="production"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check AWS CLI is installed
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    print_error "AWS credentials not configured. Please run 'aws configure' first."
    exit 1
fi

print_status "Starting AWS infrastructure setup..."

# Create S3 bucket for CSV storage
create_s3_bucket() {
    BUCKET_NAME="vectordb-csv-storage-${ENVIRONMENT}-$(date +%s)"
    print_status "Creating S3 bucket: $BUCKET_NAME"
    
    aws s3api create-bucket \
        --bucket $BUCKET_NAME \
        --region $REGION \
        --acl private || print_warning "Bucket might already exist"
    
    # Enable versioning
    aws s3api put-bucket-versioning \
        --bucket $BUCKET_NAME \
        --versioning-configuration Status=Enabled
    
    # Set lifecycle policy to delete old files
    cat <<EOF > /tmp/lifecycle.json
{
    "Rules": [
        {
            "Id": "DeleteOldFiles",
            "Status": "Enabled",
            "Expiration": {
                "Days": 30
            }
        }
    ]
}
EOF
    
    aws s3api put-bucket-lifecycle-configuration \
        --bucket $BUCKET_NAME \
        --lifecycle-configuration file:///tmp/lifecycle.json
    
    echo "S3_BUCKET_NAME=$BUCKET_NAME" >> infrastructure_outputs.txt
    print_status "S3 bucket created successfully"
}

# Create DynamoDB table
create_dynamodb_table() {
    TABLE_NAME="vectordb-tasks-${ENVIRONMENT}"
    print_status "Creating DynamoDB table: $TABLE_NAME"
    
    aws dynamodb create-table \
        --table-name $TABLE_NAME \
        --attribute-definitions \
            AttributeName=task_id,AttributeType=S \
        --key-schema \
            AttributeName=task_id,KeyType=HASH \
        --billing-mode PAY_PER_REQUEST \
        --region $REGION || print_warning "Table might already exist"
    
    # Enable point-in-time recovery
    aws dynamodb update-continuous-backups \
        --table-name $TABLE_NAME \
        --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true \
        --region $REGION || true
    
    echo "DYNAMODB_TABLE_NAME=$TABLE_NAME" >> infrastructure_outputs.txt
    print_status "DynamoDB table created successfully"
}

# Create ElastiCache Redis cluster
create_redis_cluster() {
    CLUSTER_ID="vectordb-redis-${ENVIRONMENT}"
    print_status "Creating ElastiCache Redis cluster: $CLUSTER_ID"
    
    # Get default VPC and subnet
    VPC_ID=$(aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" --query "Vpcs[0].VpcId" --output text)
    SUBNET_IDS=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" --query "Subnets[*].SubnetId" --output text | tr '\t' ',')
    
    # Create cache subnet group
    aws elasticache create-cache-subnet-group \
        --cache-subnet-group-name "vectordb-subnet-group" \
        --cache-subnet-group-description "Subnet group for VectorDB Redis" \
        --subnet-ids $(echo $SUBNET_IDS | tr ',' ' ') || true
    
    # Create Redis cluster
    aws elasticache create-cache-cluster \
        --cache-cluster-id $CLUSTER_ID \
        --engine redis \
        --engine-version "7.0" \
        --cache-node-type "cache.t3.micro" \
        --num-cache-nodes 1 \
        --cache-subnet-group-name "vectordb-subnet-group" \
        --region $REGION || print_warning "Redis cluster might already exist"
    
    # Wait for cluster to be available
    print_status "Waiting for Redis cluster to be available..."
    aws elasticache wait cache-cluster-available --cache-cluster-id $CLUSTER_ID
    
    # Get Redis endpoint
    REDIS_ENDPOINT=$(aws elasticache describe-cache-clusters \
        --cache-cluster-id $CLUSTER_ID \
        --show-cache-node-info \
        --query "CacheClusters[0].CacheNodes[0].Endpoint.Address" \
        --output text)
    
    echo "REDIS_HOST=$REDIS_ENDPOINT" >> infrastructure_outputs.txt
    print_status "Redis cluster created successfully"
}

# Create IAM role for EC2
create_iam_role() {
    ROLE_NAME="vectordb-ec2-role-${ENVIRONMENT}"
    print_status "Creating IAM role: $ROLE_NAME"
    
    # Create trust policy
    cat <<EOF > /tmp/trust-policy.json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
    
    # Create role
    aws iam create-role \
        --role-name $ROLE_NAME \
        --assume-role-policy-document file:///tmp/trust-policy.json || true
    
    # Create policy
    cat <<EOF > /tmp/policy.json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::vectordb-csv-storage-*",
                "arn:aws:s3:::vectordb-csv-storage-*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:GetItem",
                "dynamodb:UpdateItem",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:DeleteItem"
            ],
            "Resource": "arn:aws:dynamodb:*:*:table/vectordb-tasks-*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue"
            ],
            "Resource": "arn:aws:secretsmanager:*:*:secret:vectordb-api-secrets-*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "elasticache:Describe*"
            ],
            "Resource": "*"
        }
    ]
}
EOF
    
    # Attach policy to role
    aws iam put-role-policy \
        --role-name $ROLE_NAME \
        --policy-name "vectordb-policy" \
        --policy-document file:///tmp/policy.json
    
    # Create instance profile
    aws iam create-instance-profile \
        --instance-profile-name $ROLE_NAME || true
    
    # Add role to instance profile
    aws iam add-role-to-instance-profile \
        --instance-profile-name $ROLE_NAME \
        --role-name $ROLE_NAME || true
    
    echo "IAM_ROLE=$ROLE_NAME" >> infrastructure_outputs.txt
    print_status "IAM role created successfully"
}

# Create security group
create_security_group() {
    SG_NAME="vectordb-sg-${ENVIRONMENT}"
    print_status "Creating security group: $SG_NAME"
    
    VPC_ID=$(aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" --query "Vpcs[0].VpcId" --output text)
    
    # Create security group
    SG_ID=$(aws ec2 create-security-group \
        --group-name $SG_NAME \
        --description "Security group for VectorDB API" \
        --vpc-id $VPC_ID \
        --output text) || true
    
    if [ ! -z "$SG_ID" ]; then
        # Add inbound rules
        aws ec2 authorize-security-group-ingress \
            --group-id $SG_ID \
            --protocol tcp \
            --port 22 \
            --cidr 0.0.0.0/0 || true
        
        aws ec2 authorize-security-group-ingress \
            --group-id $SG_ID \
            --protocol tcp \
            --port 80 \
            --cidr 0.0.0.0/0 || true
        
        aws ec2 authorize-security-group-ingress \
            --group-id $SG_ID \
            --protocol tcp \
            --port 443 \
            --cidr 0.0.0.0/0 || true
        
        aws ec2 authorize-security-group-ingress \
            --group-id $SG_ID \
            --protocol tcp \
            --port 8000 \
            --cidr 0.0.0.0/0 || true
        
        aws ec2 authorize-security-group-ingress \
            --group-id $SG_ID \
            --protocol tcp \
            --port 5555 \
            --cidr 0.0.0.0/0 || true
        
        echo "SECURITY_GROUP_ID=$SG_ID" >> infrastructure_outputs.txt
        print_status "Security group created successfully"
    else
        print_warning "Security group might already exist"
    fi
}

# Create secrets in Secrets Manager
create_secrets() {
    SECRET_NAME="vectordb-api-secrets"
    print_status "Creating secrets in Secrets Manager..."
    
    # Create secret JSON
    cat <<EOF > /tmp/secrets.json
{
    "API_KEY": "$(openssl rand -hex 32)",
    "METABASE_API_KEY": "REPLACE_WITH_ACTUAL_KEY",
    "OPENAI_API_KEY": "REPLACE_WITH_ACTUAL_KEY",
    "PINECONE_API_KEY": "REPLACE_WITH_ACTUAL_KEY"
}
EOF
    
    # Create or update secret
    aws secretsmanager create-secret \
        --name $SECRET_NAME \
        --description "Secrets for VectorDB API" \
        --secret-string file:///tmp/secrets.json \
        --region $REGION || \
    aws secretsmanager update-secret \
        --secret-id $SECRET_NAME \
        --secret-string file:///tmp/secrets.json \
        --region $REGION
    
    rm /tmp/secrets.json
    
    print_warning "Remember to update the secret values in AWS Secrets Manager!"
    echo "SECRET_NAME=$SECRET_NAME" >> infrastructure_outputs.txt
    print_status "Secrets created successfully"
}

# Create CloudWatch log groups
create_log_groups() {
    print_status "Creating CloudWatch log groups..."
    
    aws logs create-log-group --log-group-name "/aws/ec2/vectordb-api" --region $REGION || true
    aws logs create-log-group --log-group-name "/aws/ec2/vectordb-worker" --region $REGION || true
    
    # Set retention
    aws logs put-retention-policy \
        --log-group-name "/aws/ec2/vectordb-api" \
        --retention-in-days 7 \
        --region $REGION || true
    
    aws logs put-retention-policy \
        --log-group-name "/aws/ec2/vectordb-worker" \
        --retention-in-days 7 \
        --region $REGION || true
    
    print_status "Log groups created successfully"
}

# Launch EC2 instance
launch_ec2_instance() {
    print_status "Launching EC2 instance..."
    
    # Get latest Ubuntu AMI
    AMI_ID=$(aws ec2 describe-images \
        --owners 099720109477 \
        --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*" \
        --query "Images[0].ImageId" \
        --output text)
    
    # Get security group ID
    SG_ID=$(grep SECURITY_GROUP_ID infrastructure_outputs.txt | cut -d'=' -f2)
    
    # Get IAM role
    IAM_ROLE=$(grep IAM_ROLE infrastructure_outputs.txt | cut -d'=' -f2)
    
    # Launch instance
    INSTANCE_ID=$(aws ec2 run-instances \
        --image-id $AMI_ID \
        --instance-type t3.large \
        --key-name your-key-pair \
        --security-group-ids $SG_ID \
        --iam-instance-profile Name=$IAM_ROLE \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=vectordb-api-${ENVIRONMENT}},{Key=Environment,Value=${ENVIRONMENT}}]" \
        --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":30,\"VolumeType\":\"gp3\"}}]" \
        --user-data file://user_data.sh \
        --query "Instances[0].InstanceId" \
        --output text)
    
    print_status "EC2 instance launched: $INSTANCE_ID"
    
    # Wait for instance to be running
    aws ec2 wait instance-running --instance-ids $INSTANCE_ID
    
    # Get public IP
    PUBLIC_IP=$(aws ec2 describe-instances \
        --instance-ids $INSTANCE_ID \
        --query "Reservations[0].Instances[0].PublicIpAddress" \
        --output text)
    
    echo "EC2_INSTANCE_ID=$INSTANCE_ID" >> infrastructure_outputs.txt
    echo "EC2_PUBLIC_IP=$PUBLIC_IP" >> infrastructure_outputs.txt
    
    print_status "EC2 instance is running at: $PUBLIC_IP"
}

# Main execution
main() {
    print_status "==================================================="
    print_status "AWS Infrastructure Setup for VectorDB API"
    print_status "Region: $REGION"
    print_status "Environment: $ENVIRONMENT"
    print_status "==================================================="
    
    # Clear previous outputs
    > infrastructure_outputs.txt
    
    # Create resources
    create_s3_bucket
    create_dynamodb_table
    create_iam_role
    create_security_group
    create_secrets
    create_log_groups
    create_redis_cluster
    
    print_status "==================================================="
    print_status "Infrastructure setup complete!"
    print_status "Outputs saved to: infrastructure_outputs.txt"
    print_status "==================================================="
    cat infrastructure_outputs.txt
    print_status "==================================================="
    print_warning "Next steps:"
    print_warning "1. Update secrets in AWS Secrets Manager"
    print_warning "2. Create EC2 key pair if not exists"
    print_warning "3. Launch EC2 instance with: launch_ec2_instance"
    print_warning "4. SSH to instance and run deployment script"
    print_status "==================================================="
}

# Run main
main "$@"