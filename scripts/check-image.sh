#!/bin/sh
# Boot the scratch image and confirm it contains the same executable as the OCI export.
set -eu
image=$1
platform=$2
revision=$3
executable=$4
tmp=$(mktemp -d)
container=
cleanup() {
    if [ -n "$container" ]; then docker rm -f "$container" >/dev/null 2>&1 || true; fi
    rm -rf "$tmp"
}
trap cleanup EXIT
chmod 755 "$tmp"
printf 'version = 1\n[server]\nlocal_only = true\ndrain_delay_s = 0\ngrace_s = 2\n' > "$tmp/svincolo.toml"
chmod 644 "$tmp/svincolo.toml"
test "$(docker image inspect "$image" --format '{{.Os}}/{{.Architecture}}')" = "$platform"
test "$(docker image inspect "$image" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = "$revision"
test "$(docker image inspect "$image" --format '{{.Config.User}}')" = 65532:65532
container=$(docker create --platform "$platform" --network none --read-only --user 65533:65533 \
    --cap-drop ALL --security-opt no-new-privileges --memory 192m -e HOME= \
    -v "$tmp/svincolo.toml:/config.toml:ro" "$image" serve --config /config.toml)
docker cp "$container:/svincolo" "$tmp/svincolo"
cmp "$tmp/svincolo" "$executable"
python3 - "$tmp/svincolo" "$platform" <<'PY'
from pathlib import Path
import struct
import sys

elf = Path(sys.argv[1]).read_bytes()
assert elf[:6] == b'\x7fELF\x02\x01', 'expected little-endian ELF64'
machine = struct.unpack_from('<H', elf, 18)[0]
assert machine == {'linux/amd64': 62, 'linux/arm64': 183}[sys.argv[2]]
offset = struct.unpack_from('<Q', elf, 32)[0]
size, count = struct.unpack_from('<HH', elf, 54)
for index in range(count):
    kind, _, start, _, _, length, _, _ = struct.unpack_from('<IIQQQQQQ', elf, offset + index * size)
    assert kind != 3, 'unexpected ELF interpreter'
    if kind == 2:
        for tag, _ in struct.iter_unpack('<QQ', elf[start:start + length]):
            if tag == 0:
                break
            assert tag != 1, 'unexpected dynamic library dependency'
print(f'elf=static architecture={sys.argv[2]} binary_bytes={len(elf)}')
PY
docker start "$container" >/dev/null
ready=
for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if docker exec "$container" /svincolo health --url http://127.0.0.1:8080; then
        ready=yes
        break
    fi
    sleep 1
done
test "$ready" = yes
docker stop -t 5 "$container" >/dev/null
test "$(docker inspect "$container" --format '{{.State.ExitCode}} {{.State.OOMKilled}}')" = '0 false'
printf 'scratch_start=pass network=none uid=65533 readonly=true sigterm_exit=0\n'
