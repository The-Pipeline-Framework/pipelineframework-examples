#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${EXAMPLE_DIR}/.." && pwd)"
MVN_BIN="${MVN_BIN:-${REPO_ROOT}/mvnw}"
read -r -a TPF_MAVEN_ARGUMENTS <<< "${TPF_MAVEN_ARGS:-}" || true

if [[ ! -x "${MVN_BIN}" ]]; then
  echo "ERROR: Maven wrapper not found or not executable at ${MVN_BIN}" >&2
  exit 1
fi

TPF_RESTAURANT_IMAGE="${TPF_RESTAURANT_IMAGE:-localhost/restaurant-approval/monolith-svc:latest}"
TPF_CONTAINER_STEP_TRANSPORT="${TPF_CONTAINER_STEP_TRANSPORT:-REST}"

echo "Building restaurant approval monolith container image ${TPF_RESTAURANT_IMAGE}..."
"${MVN_BIN}" -f "${EXAMPLE_DIR}/pom.xml" \
  "${TPF_MAVEN_ARGUMENTS[@]}" \
  -pl monolith-svc \
  -am \
  -DskipTests \
  -Dtpf.build.transport="${TPF_CONTAINER_STEP_TRANSPORT}" \
  -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.push=false \
  -Dquarkus.container-image.image="${TPF_RESTAURANT_IMAGE}" \
  clean package

echo "Built ${TPF_RESTAURANT_IMAGE}"
