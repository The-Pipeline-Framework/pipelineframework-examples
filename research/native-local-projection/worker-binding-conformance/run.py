"""Qualify the packaged int32 binding against one compiler-generated worker contract."""

import argparse
from collections import Counter
from hashlib import file_digest
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from zipfile import ZipFile

HERE = Path(__file__).resolve().parent
PIPELINE_ID = "native-worker-qualification"
COMPILER = "org.pipelineframework.compiler.polyglot.NativeWorkerCompiler"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def command(args, work, timeout=60):
    result = subprocess.run(args, cwd=work, capture_output=True, text=True, timeout=timeout)
    require(result.returncode == 0, f"{args[0]} failed ({result.returncode})\n{result.stdout}{result.stderr}")
    return result.stdout


def pinned_file(root, pin, suffix):
    path = (root / pin["path"]).resolve(strict=True)
    expected = pin["sha256"]
    require(path.is_file() and path.suffix == suffix, f"Expected a packaged {suffix} artifact: {path}")
    require(re.fullmatch(r"[0-9a-f]{64}", expected), f"Expected a SHA-256 pin for {path}")
    with path.open("rb") as artifact:
        require(file_digest(artifact, "sha256").hexdigest() == expected, f"Artifact digest mismatch: {path}")
    return path


def pinned_classpath(root, pins):
    jars = [pinned_file(root, pin, ".jar") for pin in pins]
    require(jars and len(jars) == len(set(jars)), "Expected a nonempty, unique packaged Java classpath")
    return os.pathsep.join(map(str, jars))


def read_worker_metadata(directory, language, type_ids):
    contract_bytes = (directory / "pipeline-contract.json").read_bytes()
    projection_bytes = (directory / "projection.json").read_bytes()
    resources = {
        "META-INF/pipeline/pipeline-contract.json": contract_bytes,
        "META-INF/pipeline/native-execution-projection.json": projection_bytes,
    }
    with ZipFile(directory / "compiled-truth.zip") as truth:
        require(Counter(truth.namelist()) == Counter(resources.keys()),
                f"Unexpected {language} Compiled Truth inventory")
        for resource, expected in resources.items():
            require(truth.read(resource) == expected, f"{language} Compiled Truth differs at {resource}")

    contract = json.loads(contract_bytes)
    projection = json.loads(projection_bytes)
    require(contract["schemaVersion"] == 2 and contract["pipelineId"] == PIPELINE_ID
            and contract["transport"] == "LOCAL"
            and contract["platform"] == "COMPUTE" and contract["runtimeLayout"] == "MONOLITH"
            and contract["pluginHost"] is False and not contract.get("module"),
            f"Unexpected {language} worker Contract")
    require(contract["capabilities"] == {
        "localTransitionExecution": True, "transitionWorkerProtocols": ["local", "rest"],
    }, f"Unexpected {language} worker capabilities")
    require(re.fullmatch(r"[0-9a-f]{64}", contract["contractHash"])
            and contract["contractVersion"] == f"sha256:{contract['contractHash']}",
            f"Unexpected {language} Contract identity")
    require(projection["kind"] == f"compiler-{language}-local"
            and projection["profile"] == "tpf-polyglot-unary-policy/1",
            f"Unexpected {language} native projection")
    require(projection["pipeline"] == {
        "id": PIPELINE_ID, "inputType": type_ids[0], "outputType": type_ids[-1],
        "transport": "LOCAL", "platform": "COMPUTE", "execution": "DIRECT",
    }, f"Unexpected {language} whole-bundle boundaries")
    require(set(contract["canonicalTypes"]) == set(type_ids)
            and Counter(row["id"] for row in projection["types"]) == Counter(type_ids),
            f"{language} catalog differs from the four canonical type IDs")
    require(all(row["fields"] == [
        {"name": "value", "type": "int32", "presence": "REQUIRED", "nullability": "NON_NULL"},
    ] for row in projection["types"]), f"Unexpected {language} canonical field declaration")
    for type_id, entry in contract["canonicalTypes"].items():
        require(entry["definition"] == {"id": type_id, "kind": "record", "fields": [
            {"name": "value", "type": {"id": "int32", "kind": "scalar"},
             "presence": "REQUIRED", "nullability": "NON_NULL"},
        ]}, f"Unexpected {language} Contract definition of {type_id}")
    contract_steps = [(step["index"], step["authoredName"], step["cardinality"],
                       step["inputTypeId"], step["outputTypeId"]) for step in contract["steps"]]
    require(all(step["kind"] == "internal" and not step.get("runtimeClass") and not step.get("clientClass")
                for step in contract["steps"]), f"{language} Contract declares Java worker classes")
    projection_steps = [(index, step["id"], "ONE_TO_ONE", step["inputType"], step["outputType"])
                        for index, step in enumerate(projection["steps"])]
    expected_steps = [(index, step, "ONE_TO_ONE", type_ids[index], type_ids[index + 1])
                      for index, step in enumerate(("A", "B", "C"))]
    require(contract_steps == projection_steps == expected_steps, f"{language} worker boundary chain differs")
    return contract_bytes, [contract_steps[0][3], *(step[4] for step in contract_steps)]


def compile_workers(work, classpath, type_ids):
    workers = {}
    contracts = {}
    for language in ("node", "python"):
        directory = work / language
        command(["java", "--enable-preview", "-cp", classpath, COMPILER, f"{language}-worker",
                 str(HERE / "pipeline.yaml"), str(HERE / "native-bindings.json"), str(directory)], work)
        contracts[language], workers[language] = read_worker_metadata(directory, language, type_ids)
    require(contracts["node"] == contracts["python"], "Node and Python generated different Contract bytes")
    require(workers["node"] == workers["python"], "Node and Python generated different boundary IDs")
    print("WORKER_METADATA_CONFORMANCE_PASS:node:python", flush=True)
    return workers["node"]


def test_cases(vectors, type_ids):
    encoding = vectors["encoding"]
    cases = []

    def add(case_id, operation, observed, *, only=None, catalog=type_ids, **arguments):
        cases.append(({"id": case_id, "operation": operation, "typeIds": catalog, **arguments}, observed, only))

    def envelope(type_id, payload, selected_encoding=encoding):
        return {"payloadTypeId": type_id, "payloadEncoding": selected_encoding, "payload": payload}

    def error(code):
        return {"error": code}

    add("catalog/accepted", "catalog", {"accepted": True})
    for row in vectors["invalidCatalogs"]:
        add(f"catalog/{row['id']}", "catalog", error("INVALID_PAYLOAD_CATALOG"), catalog=row["typeIds"])

    for unknown in vectors["unknownTypeIds"]:
        add(f"encode/unknown-type/{unknown}", "encode", error("UNKNOWN_PAYLOAD_TYPE"), typeId=unknown, value=0)

    for type_id in type_ids:
        for row in vectors["values"]:
            encoded = envelope(type_id, row["canonical"])
            add(f"encode/{type_id}/{row['id']}", "encode", encoded, typeId=type_id, value=row["value"])
            add(f"decode/{type_id}/{row['id']}", "decode", {"value": row["value"], "canonical": row["canonical"]},
                typeId=type_id, envelope=encoded)
        for row in vectors["decodes"]:
            add(f"decode/{type_id}/{row['id']}", "decode", {"value": row["value"], "canonical": row["canonical"]},
                typeId=type_id, envelope=envelope(type_id, row["payload"]))
        for row in vectors["invalidDecodes"]:
            add(f"decode/{type_id}/{row['id']}", "decode", error("INVALID_PAYLOAD_VALUE"),
                typeId=type_id, envelope=envelope(type_id, row["payload"]))

        for actual in type_ids:
            if actual != type_id:
                add(f"decode/{type_id}/known-type/{actual}", "decode", error("PAYLOAD_TYPE_MISMATCH"),
                    typeId=type_id, envelope=envelope(actual, '{"value":0}'))
        for unknown in vectors["unknownTypeIds"]:
            add(f"decode/{type_id}/unknown-actual/{unknown}", "decode", error("PAYLOAD_TYPE_MISMATCH"),
                typeId=type_id, envelope=envelope(unknown, '{"value":0}'))
            add(f"decode/{type_id}/unknown-expected/{unknown}", "decode", error("UNKNOWN_PAYLOAD_TYPE"),
                typeId=unknown, envelope=envelope(type_id, '{"value":0}'))
        for key in ("legacyEncoding", "researchEncoding", "outerEncoding"):
            add(f"decode/{type_id}/{key}", "decode", error("PAYLOAD_ENCODING_MISMATCH"),
                typeId=type_id, envelope=envelope(type_id, '{"value":0}', vectors[key]))

    # Give an otherwise wrong boundary malformed bytes. The boundary check must win over parsing.
    first, intermediate, *_ = type_ids
    add("decode/wrong-type-before-value", "decode", error("PAYLOAD_TYPE_MISMATCH"),
        typeId=first, envelope=envelope(intermediate, "{"))
    add("decode/wrong-encoding-before-value", "decode", error("PAYLOAD_ENCODING_MISMATCH"),
        typeId=first, envelope=envelope(first, "{", vectors["legacyEncoding"]))
    for row in vectors["invalidNativeEncodes"]:
        add(f"encode/native/{row['id']}", "encode", error("INVALID_PAYLOAD_VALUE"),
            only={"node", "python"}, typeId=first, value=row["value"])
    for row in vectors["invalidNativeEnvelopes"]:
        add(f"decode/native/{row['id']}", "decode", error("INVALID_PAYLOAD_ENVELOPE"),
            only={"node", "python"}, typeId=first, envelope=row["envelope"])

    legacy = envelope("java.lang.String", '"legacy"', vectors["legacyEncoding"])
    add("legacy/encode", "legacy-encode", legacy, only={"java"}, value="legacy")
    add("legacy/decode", "legacy-decode", "legacy", only={"java"}, envelope=legacy)
    add("legacy/reject-native-encoding", "legacy-decode", error(f"Unsupported transition payload encoding: {encoding}"),
        only={"java"}, envelope=envelope(first, '{"value":0}'))
    require(len({test["id"] for test, _, _ in cases}) == len(cases), "Duplicate shared payload case ID")
    return cases


def java_command(work, classpath):
    classes = work / "classes"
    classes.mkdir()
    command(["javac", "--release", "21", "-proc:none", "-cp", classpath, "-d", str(classes),
             str(HERE / "WorkerPayloadConformance.java")], work)
    return ["java", "-cp", os.pathsep.join((str(classes), classpath)), "WorkerPayloadConformance"]


def node_command(work, node_package):
    node = work / "node-probe"
    node.mkdir()
    command(["npm", "install", "--offline", "--no-save", "--package-lock=false", "--ignore-scripts",
             "--no-audit", "--no-fund", "--cache", str(work / "npm-cache"), str(node_package)], node)
    shutil.copyfile(HERE / "probe.mjs", node / "probe.mjs")
    return ["node", str(node / "probe.mjs")]


def python_command(work, wheel):
    venv = work / "venv"
    command([sys.executable, "-I", "-m", "venv", str(venv)], work)
    python = str(venv / "bin/python")
    command([python, "-I", "-m", "pip", "--isolated", "install", "--no-index", "--no-deps", str(wheel)], work)
    return [python, "-I", "-B", str(HERE / "probe.py")]


def check_report(language, probe, cases, work):
    tests = [(test, expected) for test, expected, only in cases if only is None or language in only]
    inputs = work / f"cases-{language}.json"
    inputs.write_text(json.dumps([test for test, _ in tests]), encoding="utf-8")
    args = [*probe, str(inputs)]
    if language == "java":
        args.extend(str(work / item / "pipeline-contract.json") for item in ("node", "python"))
    report = json.loads(command(args, work))
    require(len(report) == len(tests), f"{language} reported {len(report)} of {len(tests)} cases")
    for observed, (test, expected) in zip(report, tests, strict=True):
        require(observed == {"id": test["id"], "observed": expected},
                f"{language} failed {test['id']}: expected {expected!r}, observed {observed!r}")
    print(f"WORKER_PAYLOAD_CONFORMANCE_PASS:{language}:{len(tests)}", flush=True)


def run(work, manifest):
    pins = json.loads(manifest.read_bytes())
    compiler = pinned_classpath(manifest.parent, pins["compilerClasspath"])
    java = pinned_classpath(manifest.parent, pins["javaClasspath"])
    node_package = pinned_file(manifest.parent, pins["nodePackage"], ".tgz")
    wheel = pinned_file(manifest.parent, pins["pythonWheel"], ".whl")
    vectors = json.loads((HERE / "vectors.json").read_bytes())
    type_ids = compile_workers(work, compiler, vectors["boundaryTypeIds"])
    cases = test_cases(vectors, type_ids)
    check_report("java", java_command(work, java), cases, work)
    check_report("node", node_command(work, node_package), cases, work)
    check_report("python", python_command(work, wheel), cases, work)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, help="JSON with paths and SHA-256 pins for the packaged candidates")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="tpf-worker-binding-conformance-") as directory:
        run(Path(directory), args.manifest.resolve(strict=True))
