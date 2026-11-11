"""Check generated output against the current project before importing it."""
from hashlib import sha256
from importlib.machinery import all_suffixes
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT / "generated" / "project"
FIXTURES = ROOT / "fixtures"
PROFILE = "tpf-polyglot-unary-policy/1"
GENERATED_FILES = {"application.py", "models.py", "projection.json"}
RECEIPT = "generation-receipt.json"


def verify_output(
    directory: Path, files: dict[str, str], inputs: dict[str, str], *, remote: bool = False
) -> dict[str, Any]:
    expected = GENERATED_FILES | ({"diagnostics.txt"} if remote else set())
    if files.keys() != expected:
        raise ValueError("Unexpected generated file inventory.")
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("Expected a regular generated output directory.")
    inventory = {entry.name: entry for entry in directory.iterdir()}
    cache = inventory.pop("__pycache__", None)
    if cache is not None and (cache.is_symlink() or not cache.is_dir()):
        raise ValueError("Unexpected Python cache entry in generated output.")
    if inventory.keys() != expected | ({RECEIPT} if remote else set()) or not all(
        not entry.is_symlink() and entry.is_file() for entry in inventory.values()
    ):
        raise ValueError("Unexpected files or packages in generated output.")
    for name, digest in files.items():
        if sha256((directory / name).read_bytes()).hexdigest() != digest:
            raise ValueError(f"Generated file digest mismatch for {name}.")
    projection: dict[str, Any] = json.loads((directory / "projection.json").read_bytes())
    if projection["profile"] != PROFILE or projection["kind"] != "compiler-python-local":
        raise ValueError("Expected the compiler's bounded Python LOCAL projection.")
    if projection["provenance"] != {
        "dslSha256": inputs["pipeline.yaml"],
        "bindingsSha256": inputs["native-bindings.json"],
    }:
        raise ValueError("Generated projection input digests do not match generation provenance.")
    return projection


def fixture_pin(name: str) -> dict[str, Any]:
    provenance = json.loads((FIXTURES / "provenance.json").read_bytes())
    pin: dict[str, Any] = provenance["fixtures"][name]
    return pin


def verify_project(directory: Path = PROJECT, root: Path = ROOT) -> None:
    parent = directory.parent
    if parent.is_symlink() or any(
        (path.exists() or path.is_symlink())
        for suffix in all_suffixes()
        for path in (root / f"{parent.name}{suffix}", parent / f"__init__{suffix}")
    ):
        raise ValueError("Unexpected module or initializer for the generated namespace.")
    receipt_path = directory / RECEIPT
    if receipt_path.is_symlink() or (receipt_path.exists() and not receipt_path.is_file()):
        raise ValueError("Expected a regular generation receipt.")
    remote = receipt_path.is_file()
    if remote:
        receipt = json.loads(receipt_path.read_bytes())
        if (receipt["schemaVersion"], receipt["profile"], receipt["status"], receipt["target"]) != (
            1, PROFILE, "success", "python",
        ):
            raise ValueError("Expected a successful Python LOCAL generation receipt.")
        files = receipt["files"]
        source = receipt["source"]
        paths = tuple(str(Path(source["project"]) / name) for name in ("pipeline.yaml", "native-bindings.json"))
        if tuple(item["path"] for item in source["inputs"]) != paths:
            raise ValueError("Unexpected generation input paths.")
        inputs = {Path(item["path"]).name: item["sha256"] for item in source["inputs"]}
    else:
        pin = fixture_pin("project")
        files, inputs = pin["files"], pin["inputs"]
    verify_output(directory, files, inputs, remote=remote)
    for name, digest in inputs.items():
        if sha256((root / name).read_bytes()).hexdigest() != digest:
            raise ValueError(f"Generated output does not match the current {name}.")


def main() -> None:
    try:
        verify_project()
    except (KeyError, OSError, TypeError, ValueError) as error:
        raise SystemExit(f"Cannot use generated/project. {error} See the generation instructions in README.md.") from None


if __name__ == "__main__":
    main()
