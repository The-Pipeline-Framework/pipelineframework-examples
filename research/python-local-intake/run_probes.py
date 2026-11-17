"""Run the mechanism probes with a process-level deadline."""
from pathlib import Path
import subprocess
import sys


def run_with_deadline(command: list[str], *, timeout: float) -> int:
    try:
        return subprocess.run(command, timeout=timeout, check=False).returncode
    except subprocess.TimeoutExpired:
        print(f'Probe process exceeded its {timeout:g}-second deadline.', file=sys.stderr)
        return 124


if __name__ == '__main__':
    probes = Path(__file__).resolve().parent
    sys.exit(run_with_deadline([
        sys.executable, '-B', '-W', 'error', '-m', 'unittest', 'discover',
        '-s', str(probes), '-p', 'test_*.py', '-v',
    ], timeout=30))
