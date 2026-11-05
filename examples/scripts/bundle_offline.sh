#!/bin/sh
# On a connected machine: verify the pinned deoos 0.8.0 Linux archives, pull the
# pinned RustFS image, build deoos-examples:offline from the local tarball, and
# save both images to outputs/offline-bundle.tar.
set -eu
cd "$(dirname "$0")/.."

version=$(awk -F= '$1=="version"{print $2}' deoos.lock)
digest=sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff
base="https://github.com/deoos-dev/deoos/releases/download/${version}"
dest=docker/offline-tarballs
mkdir -p "$dest" outputs

echo "Downloading deoos ${version} checksums and Linux archives"
curl -fsSL --retry 3 "$base/SHA256SUMS" -o "$dest/SHA256SUMS"
for target in linux-x64 linux-arm64; do
  archive="deoos-${version}-${target}.tar.gz"
  curl -fL --retry 3 "$base/$archive" -o "$dest/$archive"
done
awk -v x64="deoos-${version}-linux-x64.tar.gz" -v arm64="deoos-${version}-linux-arm64.tar.gz" '
  $2 == x64 || $2 == arm64 { print; found++ }
  END { if (found != 2) exit 1 }
' "$dest/SHA256SUMS" > "$dest/selected.sha256"
(cd "$dest" && sha256sum -c selected.sha256)
rm -f "$dest/selected.sha256"

case "$(uname -m)" in
  x86_64) target=x64 ;;
  aarch64) target=arm64 ;;
  *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac
archive="deoos-${version}-linux-${target}.tar.gz"

echo "Pulling pinned RustFS and building deoos-examples:offline from ${archive}"
docker pull "rustfs/rustfs@${digest}"
docker build \
  --build-arg "DEOOS_TARBALL=${archive}" \
  -t deoos-examples:offline \
  -f docker/Dockerfile \
  .
docker save deoos-examples:offline "rustfs/rustfs@${digest}" -o outputs/offline-bundle.tar
echo "Wrote outputs/offline-bundle.tar"
