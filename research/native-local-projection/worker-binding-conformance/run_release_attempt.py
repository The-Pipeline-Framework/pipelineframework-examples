"""Qualify a prepared Release and pinned command in real Node and Python processes."""

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile
import tempfile
from zipfile import ZipFile

from run import COMPILER, HERE, command, compile_workers, pinned_classpath, pinned_file, require

EXAMPLES = HERE.parents[2] / "experimental"
NODE_KERNEL = Path("node_modules/@tpf/experimental-node-local")
PYTHON_KERNEL = Path("tpf_experimental_python_local")
LANGUAGES = ("node", "python")
EXPECTED_CONTEXT = {"executionId": "execution-7", "traceId": "trace-7"}


def write_member(destination, relative, content):
    require(relative.parts and not relative.is_absolute() and ".." not in relative.parts,
            f"Invalid package member {relative}")
    target = destination.joinpath(*relative.parts)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("xb") as output:
        output.write(content)


def unpack_node(package, bundle):
    with tarfile.open(package, "r:gz") as archive:
        for member in archive:
            if member.isdir():
                continue
            relative = PurePosixPath(member.name)
            require(member.isfile() and relative.parts[0] == "package",
                    f"Unexpected npm member {member.name}")
            with archive.extractfile(member) as source:
                write_member(bundle / NODE_KERNEL, PurePosixPath(*relative.parts[1:]), source.read())


def unpack_python(wheel, bundle):
    with ZipFile(wheel) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            relative = PurePosixPath(member.filename)
            require(relative.parts[0] == str(PYTHON_KERNEL)
                    or relative.parts[0].endswith(".dist-info"), f"Unexpected wheel member {member.filename}")
            if relative.parts[0] == str(PYTHON_KERNEL):
                write_member(bundle, relative, archive.read(member))


def strip_types(node, source, target, work):
    stripped = command([
        node, "--input-type=module", "--eval",
        "import {readFileSync} from 'node:fs'; import {stripTypeScriptTypes} from 'node:module';"
        "process.stdout.write(stripTypeScriptTypes(readFileSync(process.argv[1], 'utf8')));",
        str(source),
    ], work)
    target.write_text(stripped, encoding="utf-8")


def bundle_fixture(language, generated, alternate, package, work, node):
    bundle = work / f"{language}-bundle"
    app = bundle / "app"
    app.mkdir(parents=True)
    alternate_entry = work / f"{language}-alternate-application"
    if language == "node":
        (bundle / "package.json").write_text('{"type":"module"}\n', encoding="utf-8")
        for name in ("application", "models"):
            strip_types(node, generated / f"{name}.ts", app / f"{name}.js", work)
        strip_types(node, alternate / "application.ts", alternate_entry, work)
        strip_types(node, EXAMPLES / "native-node-local/src/business.ts", app / "business.js", work)
        unpack_node(package, bundle)
    else:
        project = app / "generated/project"
        project.mkdir(parents=True)
        for directory in (app, project.parent, project):
            (directory / "__init__.py").touch()
        for name in ("application", "models"):
            shutil.copyfile(generated / f"{name}.py", project / f"{name}.py")
        shutil.copyfile(alternate / "application.py", alternate_entry)
        shutil.copyfile(EXAMPLES / "native-python-local/business.py", app / "business.py")
        unpack_python(package, bundle)
    return bundle, alternate_entry


def compile_alternate(work, compiler, language):
    # A real compiler run with equal semantics but different provenance produces
    # a self-consistent embedded projection and hash that differ from original truth.
    bindings = work / f"{language}-alternate-bindings.json"
    bindings.write_bytes((HERE / "native-bindings.json").read_bytes() + b"\n")
    generated = work / f"{language}-alternate-output"
    command(["java", "--enable-preview", "-cp", compiler, COMPILER, f"{language}-worker",
             str(HERE / "pipeline.yaml"), str(bindings), str(generated)], work)
    baseline = work / language
    require((baseline / "pipeline-contract.json").read_bytes() ==
            (generated / "pipeline-contract.json").read_bytes(), "Alternate changed the logical Contract")
    require((baseline / "projection.json").read_bytes() != (generated / "projection.json").read_bytes(),
            "Alternate did not change the projection bytes")
    return generated


def create_shadow_tree(work):
    shadow = work / "ambient"
    marker = shadow / "loaded.txt"
    node_poison = ("import {writeFileSync} from 'node:fs';"
                   f"writeFileSync({json.dumps(str(marker))}, 'node');"
                   "throw new Error('ambient Node module loaded');\n").encode()
    python_poison = ("from pathlib import Path\n"
                     f"Path({str(marker)!r}).write_text('python')\n"
                     "raise RuntimeError('ambient Python module loaded')\n").encode()
    for relative in (NODE_KERNEL / "index.mjs", Path("app/business.js"), Path("preload.mjs")):
        write_member(shadow, relative, node_poison)
    write_member(shadow, NODE_KERNEL / "package.json", b'{"exports":"./index.mjs"}\n')
    for relative in (PYTHON_KERNEL / "__init__.py", Path("business.py"), Path("sitecustomize.py")):
        write_member(shadow, relative, python_poison)
    return shadow


def launch(language, node, request, cwd, inherited_environment):
    if language == "node":
        args = [node, "--input-type=module", "--eval",
                (HERE / "release_attempt.mjs").read_text(encoding="utf-8")]
    else:
        args = [sys.executable, "-I", "-S", "-B", "-c",
                (HERE / "release_attempt.py").read_text(encoding="utf-8")]
    locale = {key: inherited_environment[key] for key in ("LANG", "LC_ALL") if key in inherited_environment}
    result = subprocess.run(args, cwd=cwd, env=locale,
                            input=json.dumps(request), capture_output=True, text=True, timeout=30)
    require(result.returncode == 0, f"{language} process failed ({result.returncode})\n"
            f"{result.stdout}{result.stderr}")
    return json.loads(result.stdout)


def verify_launches(language, node, prepared, type_ids, shadow):
    imports = {
        "node": [str(NODE_KERNEL / "index.mjs"), "app/application.js", "app/models.js", "app/business.js"],
        "python": [str(PYTHON_KERNEL / "__init__.py"), "app/generated/project/application.py",
                   "app/generated/project/models.py", "app/business.py"],
    }[language]
    expected = {
        "calls": [{"handler": name, "value": value, "context": EXPECTED_CONTEXT}
                  for name, value in (("normalize", 3), ("apply", 4), ("finish", 8))],
        "payload": {"payloadTypeId": type_ids[-1], "payloadEncoding": "tpf-polyglot-int32-json/1",
                    "payload": '{"value":5}'},
        "imports": imports,
    }
    ambient = dict(os.environ, NODE_PATH=str(shadow / "node_modules"),
                   NODE_OPTIONS=f"--import={shadow / 'preload.mjs'}", PYTHONPATH=str(shadow))
    process_cases = {}
    for case, cwd, environment in (("verified", shadow.parent, os.environ), ("ambient-shadow", shadow, ambient)):
        observed = launch(language, node, prepared["verified"], cwd, environment)
        require(observed == expected, f"{language}/{case} expected {expected!r}, observed {observed!r}")
        process_cases[case] = observed
    expected = {"calls": [], "imports": imports, "error": "TRUSTED_PROJECTION_MISMATCH", "ordinaryLoadAccepted": True}
    observed = launch(language, node, prepared["projection-mismatch"], shadow, ambient)
    require(observed == expected, f"{language}/projection-mismatch expected {expected!r}, observed {observed!r}")
    require(not (shadow / "loaded.txt").exists(), f"{language} loaded an ambient module")
    process_cases["projection-mismatch"] = observed
    return process_cases


def run(work, manifest):
    pins = json.loads(manifest.read_bytes())
    compiler = pinned_classpath(manifest.parent, pins["compilerClasspath"])
    java = pinned_classpath(manifest.parent, pins["javaClasspath"])
    packages = {
        "node": pinned_file(manifest.parent, pins["nodePackage"], ".tgz"),
        "python": pinned_file(manifest.parent, pins["pythonWheel"], ".whl"),
    }
    node_command = shutil.which("node")
    require(node_command, "Node.js must be on PATH")
    node = str(Path(node_command).resolve(strict=True))
    vectors = json.loads((HERE / "vectors.json").read_bytes())
    type_ids = compile_workers(work, compiler, vectors["boundaryTypeIds"])
    classes = work / "release-classes"
    classes.mkdir()
    command(["javac", "--release", "21", "-proc:none", "-cp", java, "-d", str(classes),
             str(HERE / "NativeWorkerReleaseConformance.java")], work)
    classpath = os.pathsep.join((str(classes), java))
    shadow = create_shadow_tree(work)
    for language in LANGUAGES:
        generated = work / language
        alternate = compile_alternate(work, compiler, language)
        bundle, other_entry = bundle_fixture(language, generated, alternate, packages[language], work, node)
        evidence = json.loads(command([
            "java", "-cp", classpath, "NativeWorkerReleaseConformance",
            language, str(generated), str(bundle), str(other_entry), str(work / f"{language}-release"),
        ], work))
        process_cases = verify_launches(language, node, evidence["prepared"], type_ids, shadow)
        print(json.dumps({"language": language, "releaseReceipts": evidence["receipts"],
                          "rejectedBeforeLaunch": evidence["rejections"],
                          "processCases": process_cases}), flush=True)
        print(f"NATIVE_WORKER_RELEASE_ATTEMPT_PASS:{language}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, help="JSON with paths and SHA-256 pins for the packaged candidates")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="tpf-native-worker-release-") as directory:
        run(Path(directory), args.manifest.resolve(strict=True))
