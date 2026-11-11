"""Check nominal model/handler shape before implementing the compiler renderer."""
from pathlib import Path
import re
import subprocess
import sys

HERE = Path(__file__).resolve().parent
for filename in ('type_positive.py', 'type_negative.py'):
    expected = {(number, line.split('# expect: ', 1)[1].strip())
                for number, line in enumerate((HERE / filename).read_text().splitlines(), 1)
                if '# expect: ' in line}
    result = subprocess.run([sys.executable, '-m', 'mypy', '--strict', '--show-error-codes',
                             '--no-incremental', filename], cwd=HERE, text=True,
                            capture_output=True, timeout=60, check=False)
    output = result.stdout + result.stderr
    actual = {(int(line), code) for line, code in re.findall(
        re.escape(filename) + r':(\d+): error: .*\[([^]]+)\]', output)}
    if result.returncode != (1 if expected else 0) or actual != expected:
        raise AssertionError(f'{filename}: expected {expected}, got {actual}: {output}')
    print(output.strip())
