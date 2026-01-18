#!/bin/bash
set -e

# Paraflow Discoverer Bootstrap Script
COMPONENT="discoverer"
JOB_ID="${job_id}"
AWS_REGION="${aws_region}"
LOG_GROUP="${log_group_name}"
ENABLE_MONITORING="${enable_detailed_monitoring}"
BENCHMARK_MODE="${benchmark_mode}"
START_TIME=$(date +%s)

exec > >(tee -a /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "=== Paraflow Discoverer Bootstrap ==="
echo "Region: $AWS_REGION | Job: $JOB_ID | Bucket: ${source_bucket}"

# CloudWatch Agent (optional) - install only if monitoring enabled
if [ "$ENABLE_MONITORING" = "true" ]; then
    # CloudWatch agent may already be installed on ECS-optimized AMI
    if ! command -v /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl &> /dev/null; then
        yum install -y amazon-cloudwatch-agent
    fi
    cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json <<EOF
{"agent":{"metrics_collection_interval":10},"logs":{"logs_collected":{"files":{"collect_list":[{"file_path":"/var/log/user-data.log","log_group_name":"$LOG_GROUP","log_stream_name":"{instance_id}/$COMPONENT/user-data"}]}}},"metrics":{"namespace":"Paraflow/$JOB_ID","metrics_collected":{"cpu":{"measurement":["cpu_usage_user"],"metrics_collection_interval":10},"mem":{"measurement":["mem_used_percent"],"metrics_collection_interval":10}},"append_dimensions":{"InstanceId":"\$${aws:InstanceId}","Component":"$COMPONENT"}}}
EOF
    systemctl enable amazon-cloudwatch-agent && systemctl start amazon-cloudwatch-agent
fi

# Docker is pre-installed on ECS-Optimized AMI - just ensure it's running
systemctl start docker
for i in {1..10}; do docker info >/dev/null 2>&1 && break || sleep 1; done

# ECR Auth and pull
ECR="${ecr_repository}"
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin "$ECR"
docker pull "${ecr_repository}:${image_tag}"

# Run discoverer
echo "Starting pf-discoverer..."
CONTAINER_START=$(date +%s)

docker run --rm --name pf-discoverer -e AWS_REGION=$AWS_REGION \
  --entrypoint pf-discoverer ${ecr_repository}:${image_tag} \
  --bucket ${source_bucket} --prefix "${source_prefix}" \
  --destination sqs --sqs-queue-url ${sqs_queue_url} \
  --pattern "${file_pattern}" \
%{ if max_files > 0 ~}
  --max-files ${max_files} \
%{ endif ~}
%{ if partitioning != "" ~}
  --partitioning "${partitioning}" \
%{ endif ~}
%{ if filter != "" ~}
  --filter "${filter}" \
%{ endif ~}
  --region $AWS_REGION --progress --log-level info

EXIT_CODE=$?
DURATION=$(($(date +%s) - CONTAINER_START))

# Benchmark metrics
if [ "$BENCHMARK_MODE" = "true" ]; then
    cat > /var/log/benchmark-metrics.json <<EOF
{"component":"$COMPONENT","job_id":"$JOB_ID","instance_type":"$(curl -s http://169.254.169.254/latest/meta-data/instance-type)","duration":$DURATION,"status":"$([ $EXIT_CODE -eq 0 ] && echo SUCCESS || echo FAILED)"}
EOF
fi

echo "=== Discoverer Complete ($${DURATION}s, exit=$EXIT_CODE) ==="
exit $EXIT_CODE
