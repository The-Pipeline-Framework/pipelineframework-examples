#!/usr/bin/env bash
set -euo pipefail

suite=${1:?usage: run-system-test-suite.sh verify|restaurant-ha}
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
restaurant_container="$repo_root/restaurant-approval/self-host/container"
read -r -a maven_args <<< "${MAVEN_ARGS:-}" || true

case "$suite" in
  verify)
    "$repo_root/mvnw" -B "${maven_args[@]}" \
      -Dmaven.deploy.skip=true -Dgpg.skip=true -Dtpf.flatten.skip=true clean verify
    ;;
  restaurant-ha)
    # Build the existing demo image once, then reuse it for the three ordinary
    # self-hosted HA proofs. These stay local Docker/LocalStack exercises.
    export TPF_MAVEN_ARGS="${MAVEN_ARGS:-}"
    bash "$restaurant_container/run-container-ha-demo.sh" --prepare-images
    export TPF_SKIP_CONTAINER_BUILD=true
    export TPF_KEEP_STACK_ON_FAILURE=true
    export TPF_CI_QUIET=true
    bash "$restaurant_container/run-container-ha-demo.sh" --ci
    bash "$restaurant_container/run-container-ha-incident.sh" --ci
    bash "$restaurant_container/run-container-ha-recovery.sh" --ci
    ;;
  *)
    echo "unsupported source system-test suite: $suite" >&2
    exit 2
    ;;
esac
