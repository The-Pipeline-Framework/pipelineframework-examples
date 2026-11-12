"""Retrieve the exact Python LOCAL runtime workflow candidate."""
from collections.abc import Callable
from hashlib import sha256
import json
from pathlib import Path
import re
from shutil import rmtree
import subprocess
from tempfile import mkdtemp

from verify_project import ROOT

REPOSITORY = "The-Pipeline-Framework/pipelineframework-runtime"
WORKFLOW = ".github/workflows/experimental-python-local.yml"
WHEEL = "tpf_experimental_python_local-0.0.0-py3-none-any.whl"


def run_gh(args: list[str]) -> str:
    return subprocess.check_output(["gh", *args], text=True)


def fetch_runtime(
    pin: dict[str, str], generated: Path, gh: Callable[[list[str]], str] = run_gh
) -> Path:
    run, head, digest = pin["run"], pin["head"], pin["wheelSha256"]
    if not (re.fullmatch(r"[0-9]+", run) and re.fullmatch(r"[0-9a-f]{40}", head)
            and re.fullmatch(r"[0-9a-f]{64}", digest)):
        raise ValueError("The runtime candidate needs an exact workflow run, head SHA and wheel SHA-256.")
    destination = generated / "runtime"
    if destination.exists():
        raise ValueError("Move the existing generated/runtime directory aside before downloading.")
    version = re.match(r"gh version ([0-9]+)\.([0-9]+)", gh(["--version"]))
    if not version or tuple(map(int, version.groups())) < (2, 100):
        raise ValueError("Install GitHub CLI 2.100 or newer.")

    metadata = json.loads(gh(["api", "--hostname", "github.com", f"repos/{REPOSITORY}/actions/runs/{run}"]))
    if (
        str(metadata["id"]) != run
        or metadata["head_sha"] != head
        or metadata["path"].split("@", 1)[0] != WORKFLOW
        or metadata["head_repository"]["full_name"].lower() != REPOSITORY.lower()
        or metadata["conclusion"] != "success"
    ):
        raise ValueError("Runtime run does not match the requested successful workflow and source commit.")

    generated.mkdir(parents=True, exist_ok=True)
    staging = Path(mkdtemp(prefix="runtime-", dir=generated))
    try:
        gh([
            "run", "download", run, "--repo", f"github.com/{REPOSITORY}",
            "--name", f"python-local-runtime-{head}", "--dir", str(staging),
        ])
        inventory = list(staging.iterdir())
        if {file.name for file in inventory} != {WHEEL, "SHA256SUMS", "source-sha.txt"} or not all(
            file.is_file() and not file.is_symlink() for file in inventory
        ):
            raise ValueError("Expected exactly the runtime wheel, SHA256SUMS and source-sha.txt as regular files.")
        if (staging / "source-sha.txt").read_text().strip() != head:
            raise ValueError("The downloaded runtime candidate has a different source commit.")
        if (
            sha256((staging / WHEEL).read_bytes()).hexdigest() != digest
            or (staging / "SHA256SUMS").read_text().strip() != f"{digest}  ./{WHEEL}"
        ):
            raise ValueError("The wheel does not match the pinned SHA-256 and workflow checksum.")
        staging.rename(destination)
        return destination / WHEEL
    finally:
        if staging.exists():
            rmtree(staging)


def main() -> None:
    try:
        pin = json.loads((ROOT / "runtime-candidate.json").read_bytes())
        path = fetch_runtime(pin, ROOT / "generated")
    except (KeyError, OSError, TypeError, ValueError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"Cannot retrieve the pinned runtime candidate. Check gh authentication, Actions access and artifact expiry. {error}") from None
    print(f"Verified {path} from {REPOSITORY}@{pin['head']} (run {pin['run']}, SHA-256 {pin['wheelSha256']}).")


if __name__ == "__main__":
    main()
