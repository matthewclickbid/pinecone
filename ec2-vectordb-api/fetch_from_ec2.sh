#!/bin/bash

# Fetch the modified files from EC2 through bastion
echo "Fetching modified files from EC2..."

# First, copy files from EC2 to bastion
ssh -i ~/.ssh/cbdevelopment.pem ec2-user@54.198.220.178 << 'BASTION_EOF'
# Create temp directory on bastion
mkdir -p /tmp/ec2_files

# Copy files from EC2 to bastion
scp -i ~/aws-cbodev.pem -r ec2-user@172.18.0.218:/home/ec2-user/vectordb-api/ec2-vectordb-api/app/services/dynamodb_client.py /tmp/ec2_files/
scp -i ~/aws-cbodev.pem -r ec2-user@172.18.0.218:/home/ec2-user/vectordb-api/ec2-vectordb-api/app/utils/progress_tracker.py /tmp/ec2_files/
scp -i ~/aws-cbodev.pem -r ec2-user@172.18.0.218:/home/ec2-user/vectordb-api/ec2-vectordb-api/app/workers/chunk_processor.py /tmp/ec2_files/
BASTION_EOF

# Now copy from bastion to local
echo "Copying files from bastion to local..."
scp -i ~/.ssh/cbdevelopment.pem ec2-user@54.198.220.178:/tmp/ec2_files/dynamodb_client.py app/services/
scp -i ~/.ssh/cbdevelopment.pem ec2-user@54.198.220.178:/tmp/ec2_files/progress_tracker.py app/utils/
scp -i ~/.ssh/cbdevelopment.pem ec2-user@54.198.220.178:/tmp/ec2_files/chunk_processor.py app/workers/

echo "Files fetched successfully"
