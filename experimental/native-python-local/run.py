"""Run the installed runtime through the compiler-generated application API."""
import argparse
import asyncio
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
import json

from tpf_experimental_python_local import create_python_local_runtime

from verify_project import main as verify_project


def serialize(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}.")


def print_observation(event: Mapping[str, object]) -> None:
    print(json.dumps(dict(event), default=serialize))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("value", type=int)
    args = parser.parse_args()
    verify_project()

    from business import handlers
    from generated.project.application import InvocationContext, create_application
    from generated.project.models import InputValue

    runtime = create_python_local_runtime(on_observation=print_observation)
    application = create_application(runtime, handlers)
    terminal = asyncio.run(application.run(InputValue(args.value), InvocationContext("example", "trace-example")))
    print(json.dumps(terminal, default=serialize))
    return 0 if terminal.kind == "COMPLETED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
