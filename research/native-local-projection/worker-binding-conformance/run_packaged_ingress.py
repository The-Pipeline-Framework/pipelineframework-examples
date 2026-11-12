"""Exercise hosted HTTP and a retained terminal result across a real coordinator restart."""

import argparse
from contextlib import contextmanager
from hashlib import file_digest
from http.client import HTTPConnection
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import tempfile
from time import monotonic, sleep
from urllib.parse import urlsplit

from run_hosted_worker import (
    HERE, LANGUAGES, bundle_fixture, certificates, command, compile_alternate,
    compile_workers, host, local_port, observations, pinned_classpath, pinned_file,
    require, write_launch,
)

TOKEN = "fixture-admin-token"


def http(port, path, expected, body=None, token=TOKEN):
    connection = HTTPConnection("127.0.0.1", port, timeout=5)
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    try:
        connection.request("GET" if body is None else "POST", path,
                           None if body is None else json.dumps(body), headers)
        response = connection.getresponse()
        payload = response.read()
        require(response.status == expected, f"{path}: HTTP {response.status}, expected {expected}: {payload!r}")
        return json.loads(payload) if payload else None
    finally:
        connection.close()


def dynamo(endpoint, operation, request):
    env = os.environ | {"AWS_ACCESS_KEY_ID": "local", "AWS_SECRET_ACCESS_KEY": "local",
                        "AWS_SESSION_TOKEN": "", "AWS_PAGER": ""}
    output = subprocess.check_output(["aws", "--endpoint-url", endpoint, "--region", "us-east-1",
                                      "--output", "json", "dynamodb", operation,
                                      "--cli-input-json", json.dumps(request)], env=env, timeout=30)
    return json.loads(output)


def tables(endpoint, prefix):
    def create(suffix, keys, indexes=()):
        def schema(fields):
            return [{"AttributeName": name, "KeyType": kind}
                    for (name, _), kind in zip(fields, ("HASH", "RANGE"), strict=False)]

        fields = dict(keys)
        for _, index_keys in indexes:
            fields.update(index_keys)
        request = {
            "TableName": prefix + suffix,
            "AttributeDefinitions": [{"AttributeName": name, "AttributeType": kind} for name, kind in fields.items()],
            "KeySchema": schema(keys),
            "BillingMode": "PAY_PER_REQUEST",
        }
        if indexes:
            request["GlobalSecondaryIndexes"] = [
                {"IndexName": name, "KeySchema": schema(index_keys), "Projection": {"ProjectionType": "ALL"}}
                for name, index_keys in indexes
            ]
        dynamo(endpoint, "create-table", request)

    create("_execution", (("tenant_id", "S"), ("execution_id", "S")))
    create("_key", (("tenant_execution_key", "S"),))
    create("_payload", (("payload_id", "S"), ("payload_part", "S")))
    create("_interaction", (("tenant_id", "S"), ("interaction_id", "S")), (
        ("await-interaction-pending-by-deadline", (("query_deadline_key", "S"), ("query_deadline_sort", "S"))),
        ("await-interaction-continuation-work", (("query_continuation_key", "S"),
                                                 ("query_continuation_due_epoch_ms", "N"))),
    ))


@contextmanager
def coordinator(work, jar, launch, endpoint, prefix, secret, suffix):
    port = local_port()
    config = {
        "proof.launch": launch,
        "quarkus.http.host": "127.0.0.1",
        "quarkus.http.port": port,
        "quarkus.devservices.enabled": "false",
        "quarkus.grpc.server.use-separate-server": "false",
        "javax.net.ssl.trustStore": work / "trust.p12",
        "javax.net.ssl.trustStorePassword": "changeit",
        **{"pipeline.orchestrator." + name: value for name, value in {
            "mode": "QUEUE_ASYNC", "coordination-host": "NATIVE", "state-provider": "dynamo",
            "dispatcher-provider": "event", "sweep-interval": "PT1S",
            "idempotency-policy": "SERVER_KEY_ONLY",
            "admin.enabled": "true", "admin.admin-token": TOKEN,
            "control-plane.enabled": "true", "control-plane.admin-token": TOKEN,
            "releases.registry.provider": "file", "releases.storage.root": work / prefix,
            "dynamo.endpoint-override": endpoint, "dynamo.region": "us-east-1",
            "dynamo.execution-table": prefix + "_execution", "dynamo.execution-key-table": prefix + "_key",
            "dynamo.execution-payload-table": prefix + "_payload", "dynamo.await-interaction-table": prefix + "_interaction",
        }.items()},
    }
    args = ["java", *(f"-D{k}={v}" for k, v in config.items()), "-jar", str(jar)]
    log = work / f"{prefix}-{suffix}.log"
    env = os.environ | {"AWS_ACCESS_KEY_ID": "local", "AWS_SECRET_ACCESS_KEY": "local",
                        "AWS_SESSION_TOKEN": "", "TPF_NATIVE_WORKER_SECRET": secret}
    with log.open("w") as output, subprocess.Popen(args, cwd=work, env=env, stdout=output,
                                                    stderr=subprocess.STDOUT) as process:
        try:
            deadline = monotonic() + 45
            while process.poll() is None and monotonic() < deadline:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                        break
                except OSError:
                    sleep(0.1)
            else:
                raise AssertionError(f"Packaged coordinator failed to start:\n{log.read_text()}")
            try:
                yield port, process
            except Exception as error:
                error.add_note(log.read_text())
                raise
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def run(work, manifest, endpoint, runtime_version, maven, maven_args):
    address = urlsplit(endpoint)
    require(address.scheme == "http" and address.hostname in {"127.0.0.1", "localhost"}, "Expected local DynamoDB")
    pins = json.loads(manifest.read_bytes())
    compiler = pinned_classpath(manifest.parent, pins["compilerClasspath"])
    java = pinned_classpath(manifest.parent, pins["javaClasspath"])
    packages = {language: pinned_file(manifest.parent, pins[field], suffix)
                for language, field, suffix in (("node", "nodePackage", ".tgz"), ("python", "pythonWheel", ".whl"))}
    node = shutil.which("node")
    require(node is not None, "Node.js must be on PATH")
    compile_workers(work, compiler, json.loads((HERE / "vectors.json").read_bytes())["boundaryTypeIds"])
    certificates(work)
    classes = work / "classes"
    resources = work / "coordinator-resources"
    metadata = resources / "META-INF/pipeline"
    metadata.mkdir(parents=True)
    contract = work / "node/pipeline-contract.json"
    require(contract.read_bytes() == (work / "python/pipeline-contract.json").read_bytes(), "Contract differs by language")
    shutil.copyfile(contract, metadata / "pipeline-contract.json")
    command(["javac", "--release", "21", "-proc:none", "-cp", java, "-d", str(classes),
             str(HERE / "NativeWorkerReleaseConformance.java")], work)
    fixture = work / "coordinator"
    shutil.copytree(HERE / "coordinator", fixture, ignore=shutil.ignore_patterns("target"))
    command([maven, "-B", *maven_args, "-f", str(fixture / "pom.xml"), "package",
             f"-Dpipelineframework.runtime.version={runtime_version}", f"-Dproof.contract.resources={resources}"],
            work, timeout=180)
    jar = fixture / "target/quarkus-app/quarkus-run.jar"

    runtime_name = f"pipelineframework-{runtime_version}.jar"
    expected = next(pin["sha256"] for pin in pins["javaClasspath"] if Path(pin["path"]).name == runtime_name)
    embedded = list((jar.parent / "lib/main").glob(f"*.{runtime_name}"))
    require(len(embedded) == 1, "Expected one selected runtime JAR in the packaged coordinator")
    with embedded[0].open("rb") as artifact:
        require(file_digest(artifact, "sha256").hexdigest() == expected, "Packaged runtime differs from candidate")

    for language in LANGUAGES:
        generated = work / language
        alternate = compile_alternate(work, compiler, language)
        bundle, _ = bundle_fixture(language, generated, alternate, packages[language], work, node)
        worker_port = local_port()
        launch = json.loads(command(["java", "-cp", os.pathsep.join((str(classes), java)), "NativeWorkerReleaseConformance",
                                     language, str(generated), str(bundle), "-", str(work / f"{language}-release"),
                                     f"https://localhost:{worker_port}"], work))
        target = launch["target"]
        target["sharedSecretRef"] = "env:TPF_NATIVE_WORKER_SECRET"
        secret = secrets.token_urlsafe(32)
        calls = work / f"{language}-calls.jsonl"
        launch.update(port=worker_port, calls=str(calls), secret=secret,
                      key=str(work / "worker.key"), cert=str(work / "worker.pem"))
        launch_file = write_launch(work, language, {"target": target, "descriptor": launch["descriptor"]})
        prefix = f"native_ingress_{language}"
        tables(endpoint, prefix)
        tenant = target["tenantId"]
        pipeline = target["coordinatorContract"]["pipelineId"]
        release = target["releaseVersion"]
        admin = f"/tpf/admin/tenants/{tenant}/pipelines/{pipeline}/releases"
        control = f"/tpf/control-plane/tenants/{tenant}"
        admission = f"{control}/pipelines/{pipeline}/execution-admissions"
        intent = {"clientKey": "packaged-ingress", "contractVersion": launch["command"]["contractVersion"],
                  "releaseVersion": release, "inputShape": "UNI", "outputStreaming": False,
                  "inputPayload": {"payloadTypeId": launch["inputType"],
                                   "payloadEncoding": "tpf-polyglot-int32-json/1", "payload": '{"value":3}'}}
        with host(language, work, node, launch):
            with coordinator(work, jar, launch_file, endpoint, prefix, secret, "first") as (port, process):
                http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
                registered = http(port, f"{admin}/{release}", 200)
                http(port, admission, 401, intent | {"clientKey": "denied"}, token=None)
                http(port, admission + "/denied", 404)
                require(observations(calls) == [], "Unauthorized request reached business code")
                for suffix in ("_execution", "_key"):
                    rows = dynamo(endpoint, "scan", {"TableName": prefix + suffix, "ConsistentRead": True})
                    require(rows["Count"] == 0, "Unauthorized request wrote durable execution state")
                receipt = http(port, admission, 200, intent)
                execution = control + "/executions/" + receipt["executionId"]
                deadline = monotonic() + 30
                status = http(port, execution, 200)
                while status["status"] in {"QUEUED", "RUNNING", "WAIT_RETRY"} and monotonic() < deadline:
                    sleep(0.1)
                    status = http(port, execution, 200)
                require(status["status"] == "SUCCEEDED", f"Expected successful settlement: {status}")
                result = http(port, execution + "/result", 200)
                require(result["status"] == status and status["executionId"] == receipt["executionId"],
                        "Result changed the execution identity or status")
                require(result["resultPayload"] == {"payloadTypeId": launch["outputType"],
                        "payloadEncoding": "tpf-polyglot-int32-json/1", "payload": '{"value":5}'},
                        f"Unexpected typed result: {result}")
                first_pid = process.pid
                process.kill()
                require(process.wait(timeout=8) == -9, "Coordinator did not exit on SIGKILL")
        # Worker is offline during lookup and replay. No registration is repeated.
        with coordinator(work, jar, launch_file, endpoint, prefix, secret, "restart") as (port, process):
            require(http(port, f"{admin}/{release}", 200) == registered, "Release changed across restart")
            require(http(port, admission + "/packaged-ingress", 200) == receipt, "Retained receipt changed")
            require(http(port, admission, 200, intent) == receipt, "Replay did not return the original receipt")
            require(http(port, execution + "/result", 200) == result, "Retained result changed")
            http(port, admission, 409, intent | {"inputPayload": intent["inputPayload"] | {"payload": '{"value":4}'}})
            require(process.pid != first_pid, "Expected a fresh coordinator process")
        context = {"executionId": receipt["executionId"], "traceId": receipt["executionId"]}
        require(observations(calls) == [{"handler": name, "value": value, "context": context}
                for name, value in (("normalize", 3), ("apply", 4), ("finish", 8))], "Unexpected business calls or context")
        print(f"PACKAGED_HOSTED_INGRESS_PASS:{language}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("endpoint")
    parser.add_argument("runtime_version")
    parser.add_argument("maven")
    parser.add_argument("maven_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="tpf-packaged-ingress-") as directory:
        run(Path(directory).resolve(), args.manifest.resolve(strict=True), args.endpoint,
            args.runtime_version, args.maven, args.maven_args)
