"""Require the pinned Pyright diagnostic for a wrongly typed authored result."""
import json
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
SOURCE = HERE / 'type-errors' / 'wrong-result.py'
PYRIGHT = HERE.parents[1] / 'node_modules' / '.bin' / 'pyright'

completed = subprocess.run(
    [str(PYRIGHT), '--project', str(HERE / 'pyrightconfig.json'), '--outputjson', str(SOURCE)],
    capture_output=True, text=True, timeout=20,
)
diagnostics = json.loads(completed.stdout)['generalDiagnostics']
if completed.returncode != 1 or len(diagnostics) != 1:
    raise SystemExit(f'Expected one Pyright diagnostic, received {completed.stdout}\n{completed.stderr}')
diagnostic = diagnostics[0]
if (Path(diagnostic['file']) != SOURCE or diagnostic['rule'] != 'reportReturnType'
        or diagnostic['severity'] != 'error' or diagnostic['range']['start']['line'] != 4
        or '"str" is not assignable to "int"' not in diagnostic['message']):
    raise SystemExit(f'Expected the wrong-result type error, received {diagnostic}')
print('PYRIGHT_NEGATIVE_PASS:wrong-result')
