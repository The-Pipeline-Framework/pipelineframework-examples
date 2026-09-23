#!/usr/bin/env python3
"""Validate the untrusted workflow handoff and create the trusted source manifest/event."""
import hashlib
import json
import os
import pathlib
import re
import sys
import xml.etree.ElementTree as ET


REPOSITORY = "The-Pipeline-Framework/pipelineframework-examples"
BUILD_WORKFLOW_PATH = ".github/workflows/tpf-candidate-build.yml"
PUBLISH_WORKFLOW_PATH = ".github/workflows/tpf-candidate-publish.yml"
METADATA_FILES = {"build-metadata.json", "build-metadata.sha256"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def read_json(path: pathlib.Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"could not read candidate JSON {path}: {error}") from error
    require(isinstance(value, dict), f"candidate JSON must be an object: {path}")
    return value


def main() -> None:
    root = pathlib.Path(sys.argv[1])
    pr_path = pathlib.Path(sys.argv[2])
    root_entries = list(root.iterdir())
    require({path.name for path in root_entries} == METADATA_FILES and all(path.is_file() for path in root_entries),
            "build artifact must contain only build metadata and its checksum")
    require(not any(path.is_symlink() for path in root.iterdir()), "build artifact contains a symlink")

    metadata_path = root / "build-metadata.json"
    metadata_bytes = metadata_path.read_bytes()
    expected_checksum = f"{hashlib.sha256(metadata_bytes).hexdigest()}  build-metadata.json\n"
    require((root / "build-metadata.sha256").read_text() == expected_checksum,
            "build metadata checksum does not match the downloaded metadata")

    metadata = read_json(metadata_path)
    required = {
        "schemaVersion", "repository", "sourceRepository", "component", "sourceSha",
        "pullRequestNumber", "candidateVersion", "provenance",
    }
    require(set(metadata) == required, "build metadata has missing or unsupported fields")
    require(metadata["schemaVersion"] == 1, "unsupported build metadata schema")
    require(metadata["repository"] == REPOSITORY, "build metadata belongs to another repository")
    require(metadata["component"] == "examples", "build metadata component must be examples")
    source_sha = metadata["sourceSha"]
    require(isinstance(source_sha, str) and re.fullmatch(r"[0-9a-f]{40}", source_sha) is not None,
            "build metadata source SHA is invalid")
    require(metadata["provenance"] == {"build": {
        "repository": REPOSITORY,
        "runId": int(os.environ["BUILD_RUN_ID"]),
        "runAttempt": int(os.environ["BUILD_RUN_ATTEMPT"]),
        "workflowPath": BUILD_WORKFLOW_PATH,
        "event": os.environ["BUILD_RUN_EVENT"],
    }}, "build provenance does not match the triggering workflow_run")

    run_event = os.environ["BUILD_RUN_EVENT"]
    require(os.environ["BUILD_RUN_CONCLUSION"] == "success", "candidate build did not succeed")
    require(os.environ["BUILD_RUN_PATH"] == BUILD_WORKFLOW_PATH, "workflow_run path is not the trusted build workflow")
    require(os.environ["BUILD_RUN_REPOSITORY"].lower() == REPOSITORY.lower(),
            "workflow_run repository is not the candidate owner")
    raw_version = ET.parse("pom.xml").getroot().findtext("{http://maven.apache.org/POM/4.0.0}version", "")
    require(raw_version.endswith("-SNAPSHOT"), "trusted root project version must end in -SNAPSHOT")
    base_version = raw_version.removesuffix("-SNAPSHOT")
    require(re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", base_version) is not None,
            "trusted root project version must be plain semantic versioning")
    pr_number = metadata["pullRequestNumber"]

    if run_event == "pull_request":
        require(isinstance(pr_number, int) and pr_number > 0, "PR candidate requires a positive pull request number")
        try:
            associated_prs = json.loads(os.environ["BUILD_ASSOCIATED_PRS"])
        except json.JSONDecodeError as error:
            raise SystemExit("workflow_run pull request association is not valid JSON") from error
        require(isinstance(associated_prs, list) and len(associated_prs) > 0,
                "workflow_run must have a non-empty associated pull request list")
        associated_pr = next((candidate for candidate in associated_prs
                              if isinstance(candidate, dict) and candidate.get("number") == pr_number), None)
        require(associated_pr is not None, "candidate PR is not associated with the triggering workflow_run")
        associated_head = associated_pr.get("head")
        associated_base = associated_pr.get("base")
        require(isinstance(associated_head, dict) and associated_head.get("sha") == source_sha,
                "workflow_run associated PR head does not match candidate source SHA")
        require(isinstance(associated_base, dict), "workflow_run associated PR base is missing")
        associated_head_repo = associated_head.get("repo")
        associated_base_repo = associated_base.get("repo")
        require(isinstance(associated_head_repo, dict) and isinstance(associated_base_repo, dict),
                "workflow_run associated PR repository metadata is missing")
        require(associated_head_repo.get("full_name", "").lower() == metadata["sourceRepository"].lower(),
                "workflow_run associated PR source repository does not match build metadata")
        require(associated_base_repo.get("full_name", "").lower() == REPOSITORY.lower(),
                "workflow_run associated PR does not target the examples repository")
        require(metadata["candidateVersion"] == f"{base_version}-pr.{pr_number}.{source_sha[:12]}",
                "PR candidate version does not match its number and source SHA")
        pr = read_json(pr_path)
        require(pr.get("state") == "open", "candidate PR is no longer open")
        require(pr.get("number") == pr_number, "current PR number does not match build metadata")
        head = pr.get("head")
        base = pr.get("base")
        require(isinstance(head, dict) and isinstance(base, dict), "current PR has incomplete head/base metadata")
        head_repo = head.get("repo")
        base_repo = base.get("repo")
        require(isinstance(head_repo, dict) and isinstance(base_repo, dict), "current PR repository metadata is incomplete")
        require(base_repo.get("full_name", "").lower() == REPOSITORY.lower(),
                "current PR does not target the examples repository")
        require(head.get("sha") == source_sha, "PR head changed since the candidate build")
        require(head_repo.get("full_name", "").lower() == metadata["sourceRepository"].lower(),
                "build source repository does not match the current PR head repository")
        require(os.environ["BUILD_RUN_HEAD_BRANCH"] == head.get("ref"),
                "workflow_run branch does not match the current PR source branch")
        if head_repo["full_name"].lower() != REPOSITORY.lower():
            labels = {label.get("name") for label in pr.get("labels", []) if isinstance(label, dict)}
            require("safe-to-system-test" in labels, "fork PR lacks safe-to-system-test label")
    elif run_event == "push":
        require(pr_number is None, "main candidate must not have a pull request number")
        require(metadata["sourceRepository"].lower() == REPOSITORY.lower(),
                "main candidate source repository is not the owning repository")
        require(os.environ["BUILD_RUN_HEAD_BRANCH"] == os.environ["BUILD_RUN_DEFAULT_BRANCH"],
                "main candidate build did not run on the repository default branch")
        require(source_sha == os.environ["BUILD_RUN_HEAD_SHA"], "main candidate source SHA does not match workflow_run head")
        require(metadata["candidateVersion"] == f"{base_version}-main.{source_sha[:12]}",
                "main candidate version does not match source SHA")
    else:
        raise SystemExit(f"unsupported candidate build event: {run_event}")

    publication = {
        "repository": REPOSITORY,
        "runId": int(os.environ["GITHUB_RUN_ID"]),
        "runAttempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
        "workflowPath": PUBLISH_WORKFLOW_PATH,
        "event": "workflow_run",
    }
    manifest = {
        "schemaVersion": 1,
        "repository": REPOSITORY,
        "component": "examples",
        "sourceSha": source_sha,
        "pullRequestNumber": pr_number,
        "candidateVersion": metadata["candidateVersion"],
        "provenance": {"build": metadata["provenance"]["build"], "publication": publication},
        "mavenArtifacts": [],
        "images": [],
    }
    manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    manifest_dir = root / "candidate-manifest"
    event_dir = root / "candidate-event"
    manifest_dir.mkdir()
    event_dir.mkdir()
    (manifest_dir / "candidate-manifest.json").write_bytes(manifest_bytes)
    event = {
        "schema_version": 1,
        "source_repository": REPOSITORY,
        "source_sha": source_sha,
        "pull_request_number": pr_number,
        "component": "examples",
        "candidate_version": metadata["candidateVersion"],
        "publication_run_id": publication["runId"],
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "compatibility_set_id": None,
    }
    (event_dir / "event.json").write_text(json.dumps(event, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
