"""Qualify both packaged native workers over HTTPS with the paired Java coordinator."""

import argparse
from base64 import b64decode
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import hmac
import http.client
import json
import os
from pathlib import Path
import secrets
import select
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
from uuid import uuid4

from run import HERE, command, compile_workers, pinned_classpath, pinned_file, require
from run_release_attempt import LANGUAGES, bundle_fixture, compile_alternate

EXECUTE = "/pipeline/worker/transitions/execute"
CAPABILITIES = "/pipeline/worker/capabilities"


def local_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def certificates(work):
    # Trust a short-lived fixture CA with a DNS-only leaf so the IP address is a hostname negative.
    command(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-noenc", "-days", "1",
             "-subj", "/CN=TPF fixture CA", "-addext", "basicConstraints=critical,CA:TRUE",
             "-addext", "keyUsage=critical,keyCertSign,cRLSign",
             "-keyout", "ca.key", "-out", "ca.pem"], work)
    command(["openssl", "req", "-newkey", "rsa:2048", "-noenc", "-subj", "/CN=localhost",
             "-keyout", "worker.key", "-out", "worker.csr"], work)
    (work / "leaf.ext").write_text(
        "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n"
        "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n", encoding="ascii")
    command(["openssl", "x509", "-req", "-in", "worker.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
             "-CAcreateserial", "-days", "1", "-extfile", "leaf.ext", "-out", "worker.pem"], work)
    command(["keytool", "-importcert", "-noprompt", "-alias", "tpf-fixture-ca", "-file", "ca.pem",
             "-keystore", "trust.p12", "-storetype", "PKCS12", "-storepass", "changeit"], work)


@contextmanager
def host(language, work, node, launch):
    if language == "node":
        args = [node, str(HERE / "hosted_worker.mjs")]
    else:
        args = [sys.executable, "-I", "-S", "-B", str(HERE / "hosted_worker.py")]
    log = work / f"{language}-host.log"
    with log.open("w") as diagnostics, subprocess.Popen(
            args, cwd=work, env={}, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=diagnostics) as process:
        try:
            process.stdin.write(json.dumps(launch).encode())
            process.stdin.close()
            require(select.select([process.stdout], [], [], 15)[0] and process.stdout.readline() == b"READY\n",
                    f"{language} failed to bind HTTPS: {log.read_text()}")
            yield
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def signed(method, path, body, secret, *, timestamp=None, nonce=None):
    timestamp = timestamp or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    nonce = nonce or str(uuid4())
    canonical = "\n".join((method, path, timestamp, nonce, sha256(body).hexdigest()))
    return {
        "Content-Type": "application/json",
        "X-TPF-Worker-Timestamp": timestamp,
        "X-TPF-Worker-Nonce": nonce,
        "X-TPF-Worker-Signature": hmac.digest(secret.encode(), canonical.encode(), "sha256").hex(),
    }


def request(hostname, port, context, method, path, body, headers, *, projection_digest=None):
    client = http.client.HTTPSConnection(hostname, port, context=context, timeout=5)
    try:
        client.request(method, path, body=body, headers=headers)
        response = client.getresponse()
        if projection_digest is not None:
            require(response.headers.get_all("X-TPF-Worker-Projection-Digest") == [projection_digest],
                    "Capability response did not identify its loaded projection exactly once")
        return response.status, response.read()
    finally:
        client.close()


def observations(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def reject_before_business(work, launch):
    port, secret = launch["port"], launch["secret"]
    trusted = ssl.create_default_context(cafile=work / "ca.pem")
    caps_auth = signed("GET", CAPABILITIES, b"", secret)
    failures = {}
    for label, hostname, context in (("untrusted-ca", "localhost", ssl.create_default_context()),
                                     ("wrong-hostname", "127.0.0.1", trusted)):
        try:
            request(hostname, port, context, "GET", CAPABILITIES, b"", caps_auth)
        except ssl.SSLCertVerificationError:
            failures[label] = "certificate rejected"
        else:
            raise AssertionError(f"TLS accepted {label}")
    status, payload = request("localhost", port, trusted, "GET", CAPABILITIES, b"", caps_auth,
                              projection_digest="sha256:" + sha256(b64decode(launch["projection"], validate=True)).hexdigest())
    require(status == 200, f"Expected authenticated capabilities, got {status}")
    target = launch["target"]
    template = launch["command"]
    capability = json.loads(payload)
    require(capability == {
        "protocolVersion": "1", "providerName": "rest",
        "pipelineId": template["pipelineId"], "contractVersion": template["contractVersion"],
        "releaseVersion": target["releaseVersion"], "artifactId": target["runnableArtifactId"],
        "artifactDigest": target["runnableArtifactDigest"], "workerProtocols": ["local", "rest"],
        "payloadEncodings": ["application/tpf-transition-envelope+json"],
    }, f"Unexpected capabilities: {capability}")
    status, _ = request("localhost", port, trusted, "GET", CAPABILITIES, b"", caps_auth)
    require(status == 401, f"Duplicate nonce returned {status}")
    failures["duplicate-nonce"] = status

    cases = [
        ("bad-hmac", {}, {"secret": "wrong-secret"}, 401),
        ("expired-timestamp", {}, {"timestamp": (datetime.now(UTC) - timedelta(hours=1)).isoformat()
                                   .replace("+00:00", "Z")}, 401),
        ("wrong-tenant", {"tenantId": "another-tenant"}, {}, 400),
        ("wrong-pipeline", {"pipelineId": "another-pipeline"}, {}, 400),
        ("wrong-contract", {"contractVersion": "another-contract"}, {}, 400),
        ("wrong-release", {"releaseVersion": "another-release"}, {}, 400),
        ("wrong-start", {"currentStepIndex": 1}, {}, 400),
        ("wrong-stop", {"stopBeforeStepIndex": 1}, {}, 400),
        ("unsupported-await", {"awaitOccurrenceCause": "unsupported"}, {}, 400),
        ("out-of-range-version", {"executionVersion": 2**63}, {}, 400),
        ("wrong-encoding", {"payloadEncoding": "application/json"}, {}, 400),
        ("wrong-type", {"payloadTypeId": launch["outputType"]}, {}, 400),
        ("invalid-int32", {"payload": '{"value":2147483648}'}, {}, 400),
    ]
    for label, changes, signature, expected in cases:
        body = json.dumps(template | changes).encode()
        headers = signed("POST", EXECUTE, body, **({"secret": secret} | signature))
        status, _ = request("localhost", port, trusted, "POST", EXECUTE, body, headers)
        require(status == expected, f"{label}: expected {expected}, got {status}")
        failures[label] = status
    duplicated = b'{"tenantId":"another-tenant",' + json.dumps(template).encode()[1:]
    status, _ = request("localhost", port, trusted, "POST", EXECUTE, duplicated,
                        signed("POST", EXECUTE, duplicated, secret))
    require(status == 400, f"Duplicate envelope identity returned {status}")
    failures["duplicate-envelope-identity"] = status
    require(observations(Path(launch["calls"])) == [], "Rejected requests reached business handlers")
    return capability, failures


def verify_calls(launch, coordinator):
    expected = []
    for case, trace in coordinator["invocations"].items():
        sequence = {
            "completed": (("normalize", 3), ("apply", 4), ("finish", 8)),
            "transient": (("normalize", 10), ("apply", 11), ("apply", 11)),
            "business": (("normalize", 11), ("apply", 12)),
            "unclassified": (("normalize", 12), ("apply", 13)),
        }[case]
        expected.extend({"handler": name, "value": value, "context": trace} for name, value in sequence)
    expected.extend({"handler": name, "value": value, "context": {"executionId": "execution-7", "traceId": "trace-7"}}
                    for name, value in (("normalize", 3), ("apply", 4), ("finish", 8)))
    observed = observations(Path(launch["calls"]))
    require(observed == expected, f"Expected handler calls {expected!r}, got {observed!r}")
    return observed


def reject_replayed_transition(work, launch):
    context = ssl.create_default_context(cafile=work / "ca.pem")
    body = json.dumps(launch["command"] | {"executionVersion": 2**63 - 1}).encode()
    headers = signed("POST", EXECUTE, body, launch["secret"])
    args = ("localhost", launch["port"], context, "POST", EXECUTE, body, headers)
    status, result = request(*args)
    require(status == 200, f"Valid signed transition returned {status}")
    require(json.loads(result) == {
        "outcome": "COMPLETED", "outputPayloads": [{
            "payloadTypeId": launch["outputType"], "payloadEncoding": "tpf-polyglot-int32-json/1",
            "payload": '{"value":5}',
        }], "terminalOutputPublished": False, "terminalInputPassthrough": False,
    }, "Valid signed transition changed the portable completion")
    calls = observations(Path(launch["calls"]))
    status, _ = request(*args)
    require(status == 401, f"Replayed transition returned {status}")
    require(observations(Path(launch["calls"])) == calls, "Replayed transition ran business handlers")
    return status


def java_probe(classpath, work, launch_file, endpoint, *, trust_ca=True):
    trust = [f"-Djavax.net.ssl.trustStore={work / 'trust.p12'}", "-Djavax.net.ssl.trustStorePassword=changeit"]
    return ["java", *(trust if trust_ca else []), f"-Dpipeline.config={HERE / 'pipeline.yaml'}",
            "-cp", classpath, "org.pipelineframework.HostedWorkerCoordinatorProbe", str(launch_file), endpoint]


def java_admission_rejected(probe, work):
    report = json.loads(command([*probe, "unavailable"], work))
    require(report == {"unavailable": 503}, f"Expected Java admission rejection: {report}")
    return report["unavailable"]


def write_launch(work, name, launch):
    path = work / f"{name}-launch.json"
    path.touch(mode=0o600)
    path.write_text(json.dumps(launch))
    return path


def run(work, manifest, dynamo_endpoint):
    pins = json.loads(manifest.read_bytes())
    compiler = pinned_classpath(manifest.parent, pins["compilerClasspath"])
    java = pinned_classpath(manifest.parent, pins["javaClasspath"])
    packages = {
        "node": pinned_file(manifest.parent, pins["nodePackage"], ".tgz"),
        "python": pinned_file(manifest.parent, pins["pythonWheel"], ".whl"),
    }
    interpreter = shutil.which("node")
    require(interpreter, "Node.js must be on PATH")
    node = str(Path(interpreter).resolve(strict=True))
    vectors = json.loads((HERE / "vectors.json").read_bytes())
    compile_workers(work, compiler, vectors["boundaryTypeIds"])
    certificates(work)
    classes = work / "classes"
    classes.mkdir()
    command(["javac", "--release", "21", "-proc:none", "-cp", java, "-d", str(classes),
             str(HERE / "NativeWorkerReleaseConformance.java"), str(HERE / "HostedWorkerCoordinatorProbe.java")], work)
    metadata = classes / "META-INF/pipeline"
    metadata.mkdir(parents=True)
    shutil.copyfile(work / "node/pipeline-contract.json", metadata / "pipeline-contract.json")
    classpath = os.pathsep.join((str(classes), java))
    for language in LANGUAGES:
        generated = work / language
        alternate = compile_alternate(work, compiler, language)
        bundle, _ = bundle_fixture(language, generated, alternate, packages[language], work, node)
        port = local_port()
        launch = json.loads(command([
            "java", "-cp", classpath, "NativeWorkerReleaseConformance",
            language, str(generated), str(bundle), "-", str(work / f"{language}-release"),
            f"https://localhost:{port}",
        ], work))
        # Prepare another valid descriptor with different compiler provenance but the
        # same Contract and runnable. A separately prepared host must not satisfy it.
        different_truth = json.loads(command([
            "java", "-cp", classpath, "NativeWorkerReleaseConformance",
            language, str(alternate), str(bundle), "-", str(work / f"{language}-different-truth"),
            f"https://localhost:{port}",
        ], work))
        for field in ("coordinatorContract", "releaseVersion", "runnableArtifactId", "runnableArtifactDigest"):
            require(launch["target"][field] == different_truth["target"][field],
                    f"Projection isolation changed {field}")
        require(launch["projection"] != different_truth["projection"], "Expected distinct compiled projections")
        secret = secrets.token_urlsafe(32)
        launch.update(port=port, calls=str(work / f"{language}-calls.jsonl"), secret=secret,
                      key=str(work / "worker.key"), cert=str(work / "worker.pem"))
        launch_file = write_launch(work, language, launch)
        probe = java_probe(classpath, work, launch_file, dynamo_endpoint)
        altered = launch | {"target": launch["target"] | {"runnableArtifactDigest": "sha256:" + "1" * 64}}
        with host(language, work, node, altered):
            context = ssl.create_default_context(cafile=work / "ca.pem")
            headers = signed("GET", CAPABILITIES, b"", secret)
            status, result = request("localhost", port, context, "GET", CAPABILITIES, b"", headers)
            require(status == 200 and json.loads(result)["artifactDigest"] == altered["target"]["runnableArtifactDigest"],
                    "Mismatched worker did not advertise the alternate digest")
            wrong_artifact = java_admission_rejected(probe, work)
            require(observations(Path(launch["calls"])) == [], "Mismatched worker ran business handlers")
        diagnostics_path = work / f"{language}-coordinator.log"
        with diagnostics_path.open("w") as diagnostics:
            java_process = None
            try:
                with host(language, work, node, launch):
                    capability, rejections = reject_before_business(work, launch)
                    rejections["java-wrong-artifact"] = wrong_artifact
                    wrong_truth_file = write_launch(work, f"{language}-different-truth", launch | different_truth)
                    rejections["java-mismatched-projection"] = java_admission_rejected(
                        java_probe(classpath, work, wrong_truth_file, dynamo_endpoint), work)
                    untrusted = java_probe(classpath, work, launch_file, dynamo_endpoint, trust_ca=False)
                    rejections["java-untrusted-ca"] = java_admission_rejected(untrusted, work)
                    wrong_host = launch | {"target": launch["target"] | {"endpoint": f"https://127.0.0.1:{port}"}}
                    wrong_host_file = write_launch(work, f"{language}-wrong-host", wrong_host)
                    rejections["java-wrong-hostname"] = java_admission_rejected(
                        java_probe(classpath, work, wrong_host_file, dynamo_endpoint), work)
                    require(observations(Path(launch["calls"])) == [], "Rejected Java admission reached business handlers")
                    java_process = subprocess.Popen(probe, cwd=work, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                                    stderr=diagnostics)
                    require(select.select([java_process.stdout], [], [], 90)[0]
                            and java_process.stdout.readline() == b"DISPATCH_COMPLETE\n",
                            f"{language} dispatch failed: {diagnostics_path.read_text()}")
                    rejections["replayed-transition"] = reject_replayed_transition(work, launch)
                output, _ = java_process.communicate(input=b"OFFLINE\n", timeout=40)
                require(java_process.returncode == 0, f"{language} offline replay failed: {diagnostics_path.read_text()}")
                coordinator = json.loads(output)
                calls = verify_calls(launch, coordinator)
            finally:
                if java_process is not None and java_process.poll() is None:
                    java_process.kill()
                    java_process.wait(timeout=5)
        print(json.dumps({"language": language, "release": launch["receipt"], "capability": capability,
                          "rejections": rejections, "coordinator": coordinator, "calls": calls}), flush=True)
        print(f"NATIVE_HOSTED_WORKER_PASS:{language}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, help="JSON with paths and SHA-256 pins for the packaged candidates")
    parser.add_argument("dynamo_endpoint", help="Local DynamoDB-compatible endpoint")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="tpf-native-hosted-worker-") as directory:
        run(Path(directory).resolve(), args.manifest.resolve(strict=True), args.dynamo_endpoint)
