"""Trusted bootstrap passed to the isolated Python interpreter."""

import asyncio
from base64 import b64decode
from dataclasses import asdict
import json
from pathlib import Path
import sys


def counted_handler(name, handler, calls):
    def invoke(value, context):
        calls.append({"handler": name, "value": value.value, "context": asdict(context)})
        return handler(value, context)
    return invoke


async def run(request):
    tree = Path(request["tree"])
    sys.path[:0] = [str(tree), str(tree / "app")]
    import business
    from generated.project import application, models
    import tpf_experimental_python_local as kernel

    imports = {
        kernel: "tpf_experimental_python_local/__init__.py",
        application: "app/generated/project/application.py",
        models: "app/generated/project/models.py",
        business: "app/business.py",
    }
    for module, relative in imports.items():
        if Path(module.__file__).resolve() != tree / relative:
            raise RuntimeError(f"Prepared import changed location: {relative}")

    calls = []
    counted = {name: counted_handler(name, handler, calls) for name, handler in business.handlers.items()}
    evidence = {"calls": calls, "imports": list(imports.values())}
    if request["checkOrdinaryLoad"]:
        application.create_application(kernel.create_python_local_runtime(), counted)
        evidence["ordinaryLoadAccepted"] = True

    try:
        runtime = kernel.create_python_local_runtime(expected_projection=b64decode(request["projection"], validate=True))
        app = application.create_application(runtime, counted)
        codec = kernel.PolyglotInt32PayloadBinding(*request["typeIds"])
        value = codec.decode(request["inputType"], request["inputPayload"])
        result = await app.run(models.InputValue(value), application.InvocationContext(**request["context"]))
        if result.kind != "COMPLETED":
            raise RuntimeError(f"Expected COMPLETED, received {result!r}")
        evidence["payload"] = codec.encode(request["outputType"], result.outcome.value.value)
    except kernel.PythonLocalDiagnostic as error:
        evidence["error"] = error.code
    return evidence


print(json.dumps(asyncio.run(run(json.load(sys.stdin)))))
