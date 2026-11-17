"""Launch the packaged Python HTTPS host from a verified runnable tree."""

from base64 import b64decode
from dataclasses import asdict
import json
from pathlib import Path
import sys


def serve(input_data):
    tree = Path(input_data["tree"])
    sys.path[:0] = [str(tree), str(tree / "app")]
    import business
    from generated.project import application
    from tpf_experimental_python_local import StructuredFailure, worker

    for module, relative in ((business, "app/business.py"),
                             (application, "app/generated/project/application.py"),
                             (worker, "tpf_experimental_python_local/worker.py")):
        if Path(module.__file__).resolve() != tree / relative:
            raise RuntimeError("Prepared import changed location")

    def counted_handler(name, handler):
        def invoke(record, context):
            with open(input_data["calls"], "a", encoding="utf-8") as calls:
                calls.write(json.dumps({"handler": name, "value": record.value, "context": asdict(context)}) + "\n")
            if name == "apply" and record.value in (11, 12, 13):
                if record.value == 13:
                    raise RuntimeError("must not be sent to coordinator")
                retryable = record.value == 11
                raise StructuredFailure({"code": "TRANSIENT" if retryable else "BUSINESS_REJECTION",
                                         "message": "must not be sent to coordinator", "retryable": retryable,
                                         "owner": "LOCAL_SHELL", "stepId": "B"})
            return handler(record, context)
        return invoke

    counted = {name: counted_handler(name, handler) for name, handler in business.handlers.items()}
    target = input_data["target"]
    command = input_data["command"]
    with worker.create_native_transition_worker(
            bind=lambda runtime: application.create_application(runtime, counted),
            expected_projection=b64decode(input_data["projection"], validate=True),
            tenant_id=target["tenantId"], pipeline_id=command["pipelineId"],
            contract_version=command["contractVersion"], release_version=target["releaseVersion"],
            artifact_id=target["runnableArtifactId"], artifact_digest=target["runnableArtifactDigest"],
            profiles=target["profiles"],
            shared_secret=input_data["secret"], keyfile=input_data["key"], certfile=input_data["cert"],
            address=("127.0.0.1", input_data["port"])) as server:
        print("READY", flush=True)
        server.serve_forever()


serve(json.load(sys.stdin))
