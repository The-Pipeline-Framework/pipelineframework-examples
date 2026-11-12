"""Check both well-typed application code and wrong nominal handler boundaries."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
negative = "tests/type_errors/wrong_boundary.py"

subprocess.run([sys.executable, "-m", "mypy"], cwd=ROOT, check=True)
result = subprocess.run(
    [sys.executable, "-m", "mypy", "--no-error-summary", negative],
    cwd=ROOT, capture_output=True, text=True, check=False,
)
expected_locations = {f"{negative}:14", f"{negative}:15"}
errors = {line.split(": error:", 1)[0] for line in result.stdout.splitlines() if ": error:" in line}
if result.returncode != 1 or errors != expected_locations:
    raise SystemExit(result.stdout + result.stderr)
print("Rejected both incompatible nominal handler boundaries.")
