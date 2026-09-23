#!/usr/bin/env python3
"""Write the untrusted build handoff metadata for this source-only component."""
import hashlib
import json
import os
import pathlib
import re
import sys
import xml.etree.ElementTree as ET


REPOSITORY = "The-Pipeline-Framework/pipelineframework-examples"
WORKFLOW_PATH = ".github/workflows/tpf-candidate-build.yml"


def main() -> None:
    output = pathlib.Path(sys.argv[1])
    if output.exists():
        raise SystemExit(f"candidate build directory already exists: {output}")
    repository = os.environ["GITHUB_REPOSITORY"]
    source_repository = os.environ["SOURCE_REPOSITORY"]
    source_sha = os.environ["SOURCE_SHA"].lower()
    event = os.environ["GITHUB_EVENT_NAME"]
    pull_request = os.environ.get("PULL_REQUEST_NUMBER", "")

    if repository.lower() != REPOSITORY.lower():
        raise SystemExit("candidate build did not run in the owning repository")
    if re.fullmatch(r"[0-9a-f]{40}", source_sha) is None:
        raise SystemExit("source SHA must be a full lowercase Git SHA")

    raw_version = ET.parse("pom.xml").getroot().findtext("{http://maven.apache.org/POM/4.0.0}version", "")
    if not raw_version.endswith("-SNAPSHOT"):
        raise SystemExit("root project version must end in -SNAPSHOT")
    base_version = raw_version.removesuffix("-SNAPSHOT")
    if re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", base_version) is None:
        raise SystemExit("root project version must be plain semantic versioning")

    if event == "pull_request":
        if not re.fullmatch(r"[1-9][0-9]*", pull_request):
            raise SystemExit("pull request number is required for PR candidate metadata")
        pull_request_number = int(pull_request)
        candidate_version = f"{base_version}-pr.{pull_request_number}.{source_sha[:12]}"
    elif event == "push":
        pull_request_number = None
        if source_repository.lower() != REPOSITORY.lower():
            raise SystemExit("main candidate source repository must be the owning repository")
        candidate_version = f"{base_version}-main.{source_sha[:12]}"
    else:
        raise SystemExit(f"unsupported source candidate build event: {event}")

    metadata = {
        "schemaVersion": 1,
        "repository": REPOSITORY,
        "sourceRepository": source_repository,
        "component": "examples",
        "sourceSha": source_sha,
        "pullRequestNumber": pull_request_number,
        "candidateVersion": candidate_version,
        "provenance": {
            "build": {
                "repository": REPOSITORY,
                "runId": int(os.environ["GITHUB_RUN_ID"]),
                "runAttempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
                "workflowPath": WORKFLOW_PATH,
                "event": event,
            }
        },
    }
    encoded = (json.dumps(metadata, indent=2, sort_keys=True) + "\n").encode()
    output.mkdir(parents=True)
    (output / "build-metadata.json").write_bytes(encoded)
    (output / "build-metadata.sha256").write_text(
        f"{hashlib.sha256(encoded).hexdigest()}  build-metadata.json\n"
    )


if __name__ == "__main__":
    main()
