"""Run the shared cases against the installed Python wheel."""

import json
from pathlib import Path
import sys

from tpf_experimental_python_local import PolyglotInt32PayloadBinding, PythonLocalDiagnostic


def observe(test):
    try:
        binding = PolyglotInt32PayloadBinding(*test["typeIds"])
        match test["operation"]:
            case "catalog":
                return {"accepted": True}
            case "encode":
                return binding.encode(test["typeId"], test["value"])
            case "decode":
                type_id = test["typeId"]
                value = binding.decode(type_id, test["envelope"])
                if type(value) is not int:
                    raise TypeError(f"Unexpected decoded int32 in {test['id']}")
                return {"value": value, "canonical": binding.encode(type_id, value)["payload"]}
            case _:
                raise ValueError(f"Unexpected operation in {test['id']}")
    except PythonLocalDiagnostic as error:
        return {"error": error.code}


if __name__ == "__main__":
    cases = json.loads(Path(sys.argv[1]).read_bytes())
    print(json.dumps([{"id": test["id"], "observed": observe(test)} for test in cases]))
