"""Require actual TypeScript diagnostics; Node's type stripping is not a type checker."""
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parent
for name, diagnostic in [('missing-handler', 'TS2741'), ('wrong-handler', 'TS2322'),
                         ('missing-required', 'TS2741'), ('null-nonnullable', 'TS2322'),
                         ('undefined-optional', 'TS2375')]:
    result = subprocess.run(['node', str(root / 'node_modules/typescript/bin/tsc'),
                             '--noEmit', '--strict', '--exactOptionalPropertyTypes',
                             '--target', 'ES2022', '--module', 'NodeNext',
                             '--allowImportingTsExtensions',
                             str(root / 'type-errors' / f'{name}.ts')],
                            cwd=root, text=True, capture_output=True, timeout=30)
    output = result.stdout + result.stderr
    if result.returncode != 2 or diagnostic not in output or f'{name}.ts(' not in output:
        raise AssertionError(f'{name}: expected {diagnostic}, got {result.returncode}: {output}')
    print(f'TYPECHECK_REJECTED:{name}:{diagnostic}')
