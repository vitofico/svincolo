"""Combine the two verified BuildKit exports into one publishable OCI layout."""
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tarfile


def prepare(candidates, output):
    layout = output / "image"
    blobs = layout / "blobs" / "sha256"
    blobs.mkdir(parents=True)
    assets = output / "assets"
    assets.mkdir()
    index_type = "application/vnd.oci.image.index.v1+json"
    manifests = []
    for arch in ("amd64", "arm64"):
        candidate = candidates / arch
        with tarfile.open(candidate / f"svincolo-{arch}.oci.tar") as archive:
            archive.extractall(layout, filter="data")
        descriptors = json.loads((layout / "index.json").read_bytes())["manifests"]
        if len(descriptors) == 1 and descriptors[0]["mediaType"] == index_type:
            digest = descriptors[0]["digest"].removeprefix("sha256:")
            descriptors = json.loads((blobs / digest).read_bytes())["manifests"]
        manifests.extend(descriptors)
        for source, name in [
            ("svincolo", f"svincolo-linux-{arch}"),
            ("sbom.spdx.json", f"svincolo-linux-{arch}.spdx.json"),
        ]:
            shutil.copyfile(candidate / source, assets / name)

    # A named OCI layout entry lets skopeo copy the whole index, including
    # both platforms' original SBOM and provenance manifests, by digest.
    index = {"schemaVersion": 2, "mediaType": index_type, "manifests": manifests}
    data = json.dumps(index, separators=(",", ":")).encode()
    digest = hashlib.sha256(data).hexdigest()
    (blobs / digest).write_bytes(data)
    entry = {
        "mediaType": index_type, "digest": f"sha256:{digest}", "size": len(data),
        "annotations": {"org.opencontainers.image.ref.name": "release"},
    }
    (layout / "index.json").write_text(json.dumps({**index, "manifests": [entry]}))
    return f"sha256:{digest}"


if __name__ == "__main__":
    print(prepare(Path(sys.argv[1]), Path(sys.argv[2])))
