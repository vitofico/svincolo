#!/usr/bin/env python3
"""Verify the local OCI export and extract its tested binary and build evidence."""
import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile


archive, arch, revision, directory = sys.argv[1:]
out = Path(directory)
out.mkdir(parents=True, exist_ok=True)


def require(condition, message):
    if not condition:
        raise SystemExit(message)


with tarfile.open(archive) as oci:
    members = {m.name.removeprefix("./"): m for m in oci.getmembers()}

    def read(name):
        return oci.extractfile(members[name]).read()

    def blob(descriptor):
        algorithm, digest = descriptor["digest"].split(":")
        require(algorithm == "sha256", "unexpected OCI digest algorithm")
        data = read(f"blobs/sha256/{digest}")
        require(len(data) == descriptor["size"], "OCI descriptor size mismatch")
        require(hashlib.sha256(data).hexdigest() == digest, "OCI blob digest mismatch")
        return data

    descriptors = json.loads(read("index.json"))["manifests"]
    if len(descriptors) == 1 and descriptors[0]["mediaType"] == "application/vnd.oci.image.index.v1+json":
        descriptors = json.loads(blob(descriptors[0]))["manifests"]
    matching = [
        d for d in descriptors
        if d.get("platform", {}).get("architecture") == arch
        and d["platform"].get("os") == "linux"
    ]
    require(len(matching) == 1, "expected one Linux image for the requested architecture")
    image = matching[0]
    manifest = json.loads(blob(image))
    config = json.loads(blob(manifest["config"]))
    require(config["os"] == "linux" and config["architecture"] == arch, "OCI config platform mismatch")
    require(config["config"]["User"] == "65532:65532", "unexpected default user")
    require(config["config"]["Entrypoint"] == ["/svincolo"], "unexpected entrypoint")
    require(config["config"]["Labels"]["org.opencontainers.image.revision"] == revision,
            "OCI revision mismatch")
    layers = manifest["layers"]
    require(len(layers) == 1 and layers[0]["size"] <= 6_000_000,
            "scratch image must have one compressed layer of at most 6 MB")
    with tarfile.open(fileobj=io.BytesIO(blob(layers[0])), mode="r:*") as layer:
        files = layer.getmembers()
        require(len(files) == 1 and files[0].isfile() and files[0].name == "svincolo",
                "scratch image contains unexpected files")
        executable = layer.extractfile(files[0]).read()
    require(len(executable) <= 12_000_000, "binary exceeds 12 MB")

    statements = {}
    for descriptor in descriptors:
        if descriptor == image:
            continue
        attestation = json.loads(blob(descriptor))
        subject = attestation.get("subject", {}).get("digest")
        annotated_subject = descriptor.get("annotations", {}).get("vnd.docker.reference.digest")
        require((subject or annotated_subject) == image["digest"],
                "attestation manifest has the wrong subject")
        blob(attestation["config"])
        for layer in attestation["layers"]:
            statement = json.loads(blob(layer))
            require(statement["predicateType"] not in statements, "duplicate OCI attestation")
            subjects = statement["subject"]
            require(
                (not subjects and subject == image["digest"])
                or any(s.get("digest", {}).get("sha256") == image["digest"].split(":")[1]
                       for s in subjects),
                "attestation statement has the wrong subject",
            )
            statements[statement["predicateType"]] = statement

    sbom = statements["https://spdx.dev/Document"]["predicate"]
    required = {"svincolo", "tokio", "reqwest", "jsonschema"}
    require(required <= {p["name"] for p in sbom["packages"]},
            "image SBOM omits Rust dependencies")
    provenance = statements["https://slsa.dev/provenance/v1"]
    predicate = provenance["predicate"]
    require(any(item["uri"].startswith("pkg:docker/rust@") and item.get("digest", {}).get("sha256")
                for item in predicate["buildDefinition"]["resolvedDependencies"]),
            "missing pinned Rust build material")
    vcs = predicate["runDetails"]["metadata"]["buildkit_metadata"]["vcs"]
    require(vcs["revision"] == revision, "provenance revision mismatch")

(out / "svincolo").write_bytes(executable)
(out / "svincolo").chmod(0o755)
(out / "sbom.spdx.json").write_text(json.dumps(sbom, indent=2) + "\n")
(out / "provenance.intoto.json").write_text(json.dumps(provenance, indent=2) + "\n")
print(f"manifest={image['digest']} config={manifest['config']['digest']} "
      f"binary_bytes={len(executable)} compressed_layer_bytes={layers[0]['size']} "
      f"sbom_packages={len(sbom['packages'])}")
