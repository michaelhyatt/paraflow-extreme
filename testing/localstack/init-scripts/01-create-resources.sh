#!/bin/bash
# Create S3 bucket and SQS queues for testing

set -e

echo "Creating S3 bucket..."
awslocal s3 mb s3://test-bucket

echo "Creating SQS work queue..."
awslocal sqs create-queue --queue-name work-queue

echo "Creating SQS dead letter queue..."
awslocal sqs create-queue --queue-name work-queue-dlq

echo "Setting up redrive policy..."
DLQ_ARN=$(awslocal sqs get-queue-attributes \
    --queue-url http://localhost:4566/000000000000/work-queue-dlq \
    --attribute-names QueueArn \
    --query 'Attributes.QueueArn' \
    --output text)

awslocal sqs set-queue-attributes \
    --queue-url http://localhost:4566/000000000000/work-queue \
    --attributes '{
        "RedrivePolicy": "{\"deadLetterTargetArn\":\"'$DLQ_ARN'\",\"maxReceiveCount\":\"3\"}"
    }'

echo "LocalStack resources created successfully!"
echo ""
echo "S3 Bucket: s3://test-bucket"
echo "SQS Queue URL: http://localhost:4566/000000000000/work-queue"
echo "SQS DLQ URL: http://localhost:4566/000000000000/work-queue-dlq"
