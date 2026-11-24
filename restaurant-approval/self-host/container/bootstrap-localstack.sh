#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"

export TPF_REPO_ROOT="${TPF_REPO_ROOT:-${REPO_ROOT}}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

compose_up() {
  if [[ "${TPF_CI_QUIET:-false}" == "true" ]]; then
    compose up --quiet-pull "$@"
    return
  fi
  compose up "$@"
}

awslocal() {
  compose exec -T localstack awslocal "$@"
}

wait_for_localstack() {
  echo "Waiting for LocalStack..."
  for _ in {1..120}; do
    if compose exec -T localstack curl -fsS "http://localhost:4566/_localstack/health" >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  echo "Timed out waiting for LocalStack." >&2
  compose logs localstack >&2 || true
  exit 1
}

validate_existing_await_interaction() {
  python3 - "$1" <<'PY_SCHEMA'
import json
import sys

table = json.loads(sys.argv[1]).get("Table", {})
attributes = {item["AttributeName"]: item["AttributeType"] for item in table.get("AttributeDefinitions", [])}
def keys(items):
    return {item["KeyType"]: item["AttributeName"] for item in items}
expected = {
    "await-interaction-by-unit": ("query_unit_key", "query_unit_sort"),
    "await-interaction-pending-by-tenant": ("query_pending_tenant_key", "query_pending_deadline_sort"),
    "await-interaction-pending-by-assignee": ("query_pending_assignee_key", "query_pending_deadline_sort"),
    "await-interaction-pending-by-group": ("query_pending_group_key", "query_pending_deadline_sort"),
    "await-interaction-pending-by-step": ("query_pending_step_key", "query_pending_deadline_sort"),
    "await-interaction-pending-by-deadline": ("query_deadline_key", "query_deadline_sort"),
    "await-interaction-continuation-work": ("query_continuation_key", "query_continuation_due_epoch_ms"),
    "await-interaction-by-execution": ("query_execution_key", "query_execution_sort"),
}
errors = []
if table.get("TableStatus") != "ACTIVE" or keys(table.get("KeySchema", [])) != {"HASH": "tenant_id", "RANGE": "interaction_id"}:
    errors.append("table must be ACTIVE with tenant_id/interaction_id keys")
for name in ("tenant_id", "interaction_id"):
    if attributes.get(name) != "S":
        errors.append(name + " must have type S")
indexes = {item["IndexName"]: item for item in table.get("GlobalSecondaryIndexes", [])}
for name, (partition, sort) in expected.items():
    index = indexes.get(name, {})
    if (index.get("IndexStatus") != "ACTIVE"
        or keys(index.get("KeySchema", [])) != {"HASH": partition, "RANGE": sort}
        or index.get("Projection", {}).get("ProjectionType") != "ALL"):
        errors.append(name + " must be ACTIVE with the required keys and ALL projection")
    for attribute in (partition, sort):
        required = "N" if attribute == "query_continuation_due_epoch_ms" else "S"
        if attributes.get(attribute) != required:
            errors.append(attribute + " must have type " + required)
if errors:
    print("Existing tpf_await_interaction schema is incompatible: " + "; ".join(errors), file=sys.stderr)
    sys.exit(1)
PY_SCHEMA
}

create_table_if_missing() {
  local table_name="$1"
  shift
  local description
  if description="$(awslocal dynamodb describe-table --table-name "${table_name}" 2>/dev/null)"; then
    if [[ "${table_name}" == "tpf_await_interaction" ]] && ! validate_existing_await_interaction "${description}"; then
      echo "ERROR: Retained LocalStack table was not modified; no reset was performed." >&2
      echo "If a table or index is still starting, wait for ACTIVE and rerun bootstrap." >&2
      echo "If the local demo data may be discarded, reset it yourself, then rerun bootstrap:" >&2
      echo "  docker compose -f \"${COMPOSE_FILE}\" down -v" >&2
      return 1
    fi
    echo "DynamoDB table exists: ${table_name}"
    return
  fi
  echo "Creating DynamoDB table: ${table_name}"
  awslocal dynamodb create-table --table-name "${table_name}" "$@" >/dev/null
  awslocal dynamodb wait table-exists --table-name "${table_name}"
}

create_queue_if_missing() {
  local queue_name="$1"
  if awslocal sqs get-queue-url --queue-name "${queue_name}" >/dev/null 2>&1; then
    echo "SQS queue exists: ${queue_name}"
    return
  fi
  echo "Creating SQS queue: ${queue_name}"
  awslocal sqs create-queue --queue-name "${queue_name}" >/dev/null
}

create_bucket_if_missing() {
  local bucket="$1"
  if awslocal s3api head-bucket --bucket "${bucket}" >/dev/null 2>&1; then
    echo "S3 bucket exists: ${bucket}"
    return
  fi
  echo "Creating S3 bucket: ${bucket}"
  awslocal s3api create-bucket --bucket "${bucket}" >/dev/null
}

compose_up -d localstack
wait_for_localstack

create_table_if_missing tpf_execution \
  --attribute-definitions \
    AttributeName=tenant_id,AttributeType=S \
    AttributeName=execution_id,AttributeType=S \
  --key-schema \
    AttributeName=tenant_id,KeyType=HASH \
    AttributeName=execution_id,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_execution_key \
  --attribute-definitions AttributeName=tenant_execution_key,AttributeType=S \
  --key-schema AttributeName=tenant_execution_key,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_await_unit \
  --attribute-definitions \
    AttributeName=tenant_id,AttributeType=S \
    AttributeName=unit_id,AttributeType=S \
  --key-schema \
    AttributeName=tenant_id,KeyType=HASH \
    AttributeName=unit_id,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_await_interaction \
  --attribute-definitions \
    AttributeName=tenant_id,AttributeType=S \
    AttributeName=interaction_id,AttributeType=S \
    AttributeName=query_unit_key,AttributeType=S \
    AttributeName=query_unit_sort,AttributeType=S \
    AttributeName=query_pending_tenant_key,AttributeType=S \
    AttributeName=query_pending_assignee_key,AttributeType=S \
    AttributeName=query_pending_group_key,AttributeType=S \
    AttributeName=query_pending_step_key,AttributeType=S \
    AttributeName=query_pending_deadline_sort,AttributeType=S \
    AttributeName=query_deadline_key,AttributeType=S \
    AttributeName=query_deadline_sort,AttributeType=S \
    AttributeName=query_continuation_key,AttributeType=S \
    AttributeName=query_continuation_due_epoch_ms,AttributeType=N \
    AttributeName=query_execution_key,AttributeType=S \
    AttributeName=query_execution_sort,AttributeType=S \
  --key-schema \
    AttributeName=tenant_id,KeyType=HASH \
    AttributeName=interaction_id,KeyType=RANGE \
  --global-secondary-indexes \
    'IndexName=await-interaction-by-unit,KeySchema=[{AttributeName=query_unit_key,KeyType=HASH},{AttributeName=query_unit_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=await-interaction-pending-by-tenant,KeySchema=[{AttributeName=query_pending_tenant_key,KeyType=HASH},{AttributeName=query_pending_deadline_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=await-interaction-pending-by-assignee,KeySchema=[{AttributeName=query_pending_assignee_key,KeyType=HASH},{AttributeName=query_pending_deadline_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=await-interaction-pending-by-group,KeySchema=[{AttributeName=query_pending_group_key,KeyType=HASH},{AttributeName=query_pending_deadline_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=await-interaction-pending-by-step,KeySchema=[{AttributeName=query_pending_step_key,KeyType=HASH},{AttributeName=query_pending_deadline_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=await-interaction-pending-by-deadline,KeySchema=[{AttributeName=query_deadline_key,KeyType=HASH},{AttributeName=query_deadline_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=await-interaction-continuation-work,KeySchema=[{AttributeName=query_continuation_key,KeyType=HASH},{AttributeName=query_continuation_due_epoch_ms,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=await-interaction-by-execution,KeySchema=[{AttributeName=query_execution_key,KeyType=HASH},{AttributeName=query_execution_sort,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_await_interaction_key \
  --attribute-definitions AttributeName=lookup_key,AttributeType=S \
  --key-schema AttributeName=lookup_key,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_await_completion_lifecycle \
  --attribute-definitions \
    AttributeName=pk,AttributeType=S \
    AttributeName=sk,AttributeType=S \
  --key-schema \
    AttributeName=pk,KeyType=HASH \
    AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_release_registry \
  --attribute-definitions \
    AttributeName=registry_key,AttributeType=S \
    AttributeName=registry_sort,AttributeType=S \
  --key-schema \
    AttributeName=registry_key,KeyType=HASH \
    AttributeName=registry_sort,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

create_table_if_missing tpf_worker_registry \
  --attribute-definitions \
    AttributeName=registry_key,AttributeType=S \
    AttributeName=registry_sort,AttributeType=S \
  --key-schema \
    AttributeName=registry_key,KeyType=HASH \
    AttributeName=registry_sort,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

create_queue_if_missing tpf-work
create_queue_if_missing tpf-execution-dlq
create_bucket_if_missing tpf-release-artifacts

echo "LocalStack bootstrap complete."
