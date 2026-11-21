#!/usr/bin/env python3
"""Prove Expansion dependencies and processor Blocks use the same candidate.

Uses tiny local Maven artifacts, not a framework build or published candidates.
"""
import copy
import json
import pathlib
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
LOCAL_REPO = ROOT / ".m2/repository"
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
ET.register_namespace("", NS["m"])
BASELINE = "0.0.0-bom-alignment-baseline"
CANDIDATE = "0.0.0-bom-alignment-candidate"


def local_artifact(group, artifact, version, packaging="jar", body=""):
    directory = LOCAL_REPO / group.replace(".", "/") / artifact / version
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{artifact}-{version}.pom").write_text(
        f"<project><modelVersion>4.0.0</modelVersion><groupId>{group}</groupId>"
        f"<artifactId>{artifact}</artifactId><version>{version}</version>"
        f"<packaging>{packaging}</packaging>{body}</project>"
    )
    if packaging == "jar":
        with zipfile.ZipFile(directory / f"{artifact}-{version}.jar", "w") as jar:
            jar.writestr("META-INF/pipeline/blocks.json", json.dumps({"version": version}))


def verify_restaurant_contract_alignment():
    """Resolve actual authored properties/dependencies offline with distinct BOM and Contracts identities."""
    root = ET.parse(ROOT / "pom.xml").getroot()
    restaurant = ET.parse(ROOT / "restaurant-approval/pom.xml").getroot()
    worker = ET.parse(ROOT / "restaurant-approval/worker-lambda-svc/pom.xml").getroot()
    bom_version = "0.0.0-restaurant-probe-bom"
    contracts_version = "0.0.0-restaurant-probe-contracts"
    explicit_version = "0.0.0-restaurant-probe-explicit"
    local_artifact("org.pipelineframework", "pipelineframework-bom", bom_version, "pom")
    release_plugin = next(plugin for plugin in worker.findall("m:build/m:plugins/m:plugin", NS)
                          if plugin.findtext("m:artifactId", namespaces=NS) == "pipelineframework-release-maven-plugin")
    managed_contracts = [dependency for dependency in restaurant.findall("m:dependencyManagement/m:dependencies/m:dependency", NS)
                         if dependency.findtext("m:version", namespaces=NS) == "${restaurant.contracts.version}"]
    assert len(managed_contracts) == 9
    with tempfile.TemporaryDirectory(prefix="tpf-restaurant-contract-alignment-") as temporary:
        directory = pathlib.Path(temporary)
        parent = ET.Element("project")
        for name, value in (("modelVersion", "4.0.0"), ("groupId", "test"), ("artifactId", "contract-parent"),
                            ("version", "1"), ("packaging", "pom")):
            ET.SubElement(parent, name).text = value
        properties = ET.SubElement(parent, "properties")
        for name in ("pipelineframework.bom.version", "pipelineframework.contracts.version", "pipelineframework.runtime.version"):
            authored = root.find("m:properties/m:" + name, NS)
            assert authored is not None, "root must expose the canonical Contracts compatibility override"
            properties.append(copy.deepcopy(authored))
        imports = ET.SubElement(ET.SubElement(parent, "dependencyManagement"), "dependencies")
        imports.append(copy.deepcopy(next(dependency for dependency in root.findall("m:dependencyManagement/m:dependencies/m:dependency", NS)
                                         if dependency.findtext("m:artifactId", namespaces=NS) == "pipelineframework-bom")))
        ET.ElementTree(parent).write(directory / "parent.xml", encoding="unicode")
        child = ET.Element("project")
        ET.SubElement(child, "modelVersion").text = "4.0.0"
        inheritance = ET.SubElement(child, "parent")
        for name, value in (("groupId", "test"), ("artifactId", "contract-parent"), ("version", "1"), ("relativePath", "parent.xml")):
            ET.SubElement(inheritance, name).text = value
        ET.SubElement(child, "artifactId").text = "restaurant-contract-model"
        properties = ET.SubElement(child, "properties")
        for name in ("restaurant.contracts.version", "restaurant.release.plugin.version"):
            properties.append(copy.deepcopy(restaurant.find("m:properties/m:" + name, NS)))
        management = ET.SubElement(ET.SubElement(child, "dependencyManagement"), "dependencies")
        for dependency in managed_contracts:
            management.append(copy.deepcopy(dependency))
        dependencies = ET.SubElement(child, "dependencies")
        dependency = ET.SubElement(dependencies, "dependency")
        ET.SubElement(dependency, "groupId").text = "org.pipelineframework"
        ET.SubElement(dependency, "artifactId").text = "pipelineframework-runtime-core"
        plugin = ET.SubElement(ET.SubElement(ET.SubElement(child, "build"), "plugins"), "plugin")
        for name in ("groupId", "artifactId", "version", "dependencies"):
            plugin.append(copy.deepcopy(release_plugin.find("m:" + name, NS)))

        def resolve(arguments, expected):
            ET.ElementTree(child).write(directory / "pom.xml", encoding="unicode")
            subprocess.run([str(ROOT / "mvnw"), "-o", "-N", "-f", str(directory / "pom.xml"),
                            "org.apache.maven.plugins:maven-help-plugin:3.5.2:effective-pom",
                            f"-Doutput={directory / 'effective.xml'}", f"-Dmaven.repo.local={LOCAL_REPO}",
                            f"-Dpipelineframework.bom.version={bom_version}", *arguments], cwd=ROOT, check=True)
            effective = ET.parse(directory / "effective.xml").getroot()
            selected = effective.findtext("m:dependencies/m:dependency/m:version", namespaces=NS)
            assert selected == expected, f"application Contracts pin {selected} != {expected}"
            actual_plugin = next(item for item in effective.findall("m:build/m:plugins/m:plugin", NS)
                                 if item.findtext("m:artifactId", namespaces=NS) == "pipelineframework-release-maven-plugin")
            versions = [item.findtext("m:version", namespaces=NS) for item in actual_plugin.findall("m:dependencies/m:dependency", NS)]
            assert versions == [expected] * 9, f"producer reader graph did not inherit Contracts pin: {versions}"

        resolve([f"-Dpipelineframework.contracts.version={contracts_version}"], contracts_version)
        resolve([f"-Dpipelineframework.contracts.version={contracts_version}", f"-Drestaurant.contracts.version={explicit_version}"], explicit_version)
        resolve([], root.findtext("m:properties/m:pipelineframework.contracts.version", namespaces=NS))
        properties.find("m:restaurant.contracts.version", NS).text = "${pipelineframework.bom.version}"
        resolve([f"-Dpipelineframework.contracts.version={contracts_version}"], bom_version)
    print("Restaurant Contracts alignment: independent canonical/explicit/fallback pins reach application and plugin models; old BOM alias reproduced")


def main():
    parent = ET.parse(ROOT / "pom.xml").getroot()
    contracts_versions = parent.findall("m:properties/m:pipelineframework.contracts.version", NS)
    assert len(contracts_versions) == 1 and (contracts_versions[0].text or "").strip(), (
        "examples root POM must declare pipelineframework.contracts.version exactly once"
    )
    imports = parent.findall("m:dependencyManagement/m:dependencies/m:dependency", NS)
    bom = [dependency for dependency in imports
           if dependency.findtext("m:artifactId", namespaces=NS) == "pipelineframework-bom"]
    assert len(bom) == 1, "examples must import the tested TPF BOM exactly once"
    assert bom[0].findtext("m:version", namespaces=NS) == "${pipelineframework.bom.version}"
    assert bom[0].findtext("m:type", namespaces=NS) == "pom"
    assert bom[0].findtext("m:scope", namespaces=NS) == "import"

    proof = ET.parse(ROOT / "graphql-block-proof/pom.xml").getroot()
    expansion = next(dependency for dependency in proof.findall("m:dependencies/m:dependency", NS)
                     if dependency.findtext("m:groupId", namespaces=NS) == "org.pipelineframework.expansions")
    processor = next(path for path in proof.findall(".//m:annotationProcessorPaths/m:path", NS)
                     if path.findtext("m:artifactId", namespaces=NS) == "graphql-agent")
    assert processor.findtext("m:version", namespaces=NS) == "${pipelineframework.blocks.version}"

    block_dependency = (
        "<dependency><groupId>org.pipelineframework.blocks</groupId>"
        "<artifactId>graphql-agent</artifactId>"
        f"<version>{BASELINE}</version></dependency>"
    )
    for version in (BASELINE, CANDIDATE):
        local_artifact("org.pipelineframework.blocks", "graphql-agent", version)
    local_artifact("org.pipelineframework.expansions", "graphql", BASELINE, "pom",
                   f"<dependencies>{block_dependency}</dependencies>")
    local_artifact("org.pipelineframework", "pipelineframework-bom", CANDIDATE, "pom",
                   "<dependencyManagement><dependencies>"
                   + block_dependency.replace(BASELINE, CANDIDATE)
                   + "</dependencies></dependencyManagement>")

    with tempfile.TemporaryDirectory(prefix="tpf-bom-alignment-") as temporary:
        directory = pathlib.Path(temporary)
        project = ET.Element("project")
        for name, value in (("modelVersion", "4.0.0"), ("groupId", "test"),
                            ("artifactId", "block-overlay"), ("version", "1")):
            ET.SubElement(project, name).text = value
        properties = ET.SubElement(project, "properties")
        for name, value in (("pipelineframework.bom.version", CANDIDATE),
                            ("pipelineframework.blocks.version", CANDIDATE),
                            ("pipelineframework.expansions.version", BASELINE)):
            ET.SubElement(properties, name).text = value
        managed = ET.SubElement(ET.SubElement(project, "dependencyManagement"), "dependencies")
        managed.append(copy.deepcopy(bom[0]))
        ET.SubElement(project, "dependencies").append(copy.deepcopy(expansion))
        plugin = ET.SubElement(ET.SubElement(ET.SubElement(project, "build"), "plugins"), "plugin")
        ET.SubElement(plugin, "artifactId").text = "maven-compiler-plugin"
        ET.SubElement(plugin, "version").text = "3.14.0"
        paths = ET.SubElement(ET.SubElement(plugin, "configuration"), "annotationProcessorPaths")
        paths.append(copy.deepcopy(processor))
        pom = directory / "pom.xml"

        def resolve():
            ET.ElementTree(project).write(pom, encoding="unicode")
            subprocess.run([
                str(ROOT / "mvnw"), "-B", "-f", str(pom),
                "org.apache.maven.plugins:maven-dependency-plugin:3.7.0:tree",
                "-DoutputType=json", f"-DoutputFile={directory / 'tree.json'}",
                "org.apache.maven.plugins:maven-help-plugin:3.5.2:effective-pom",
                f"-Doutput={directory / 'effective.xml'}", "--no-transfer-progress",
                f"-Dmaven.repo.local={LOCAL_REPO}"
            ], cwd=ROOT, check=True)
            tree = json.loads((directory / "tree.json").read_text())
            return tree["children"][0]["children"][0]["version"]

        assert resolve() == CANDIDATE, "Expansion must resolve the selected candidate Block"
        effective = ET.parse(directory / "effective.xml").getroot()
        path = effective.find(".//m:annotationProcessorPaths/m:path/m:version", NS)
        assert path is not None and path.text == CANDIDATE
        project.remove(project.find("dependencyManagement"))
        assert resolve() == BASELINE, "control must reproduce the old baseline/candidate mismatch"
    print("BOM alignment: candidate replaces the baseline Block on the dependency path")
    verify_restaurant_contract_alignment()


if __name__ == "__main__":
    main()
