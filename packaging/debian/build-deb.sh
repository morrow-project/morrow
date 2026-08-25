#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 4 ]]; then
  echo "usage: $0 VERSION DEBIAN_ARCH ARCHIVE OUTPUT_DIR" >&2
  exit 2
fi

version="$1"
debian_arch="$2"
archive="$3"
output_dir="$4"
package_root="$(mktemp -d)"
trap 'rm -rf "${package_root}"' EXIT
script_dir="$(cd "$(dirname "$0")" && pwd)"
unit_file="${script_dir}/../systemd/morrow.service"

if [[ ! -r "${unit_file}" ]]; then
  echo "missing shared systemd unit: ${unit_file}" >&2
  exit 1
fi

name="morrow"
package_dir="${package_root}/${name}"
mkdir -p "${package_dir}/DEBIAN" \
  "${package_dir}/etc/morrow" \
  "${package_dir}/lib/systemd/system" \
  "${package_dir}/usr/bin" \
  "${package_dir}/usr/share/doc/${name}"

tar -xzf "${archive}" -C "${package_root}"
release_dir="${package_root}/morrow-${version}-linux-${debian_arch}"

install -m 0755 "${release_dir}/morrow-server" "${package_dir}/usr/bin/morrow-server"
install -m 0755 "${release_dir}/morrow-cli" "${package_dir}/usr/bin/morrow-cli"
install -m 0755 "${release_dir}/morrow-connector" "${package_dir}/usr/bin/morrow-connector"
install -m 0644 "${release_dir}/morrow.json.example" "${package_dir}/etc/morrow/morrow.json.example"
install -m 0644 "${release_dir}/client.json.example" "${package_dir}/usr/share/doc/${name}/client.json.example"
install -m 0644 "${release_dir}/LICENSE" "${package_dir}/usr/share/doc/${name}/LICENSE"
install -m 0644 "${release_dir}/README.md" "${package_dir}/usr/share/doc/${name}/README.md"
install -m 0644 "${release_dir}/docs/building.md" "${package_dir}/usr/share/doc/${name}/building.md"
install -m 0644 "${release_dir}/docs/operations.md" "${package_dir}/usr/share/doc/${name}/operations.md"
install -m 0644 "${script_dir}/../../docs/packaging-systemd.md" "${package_dir}/usr/share/doc/${name}/packaging-systemd.md"
install -m 0644 "${unit_file}" "${package_dir}/lib/systemd/system/morrow.service"

cat >"${package_dir}/DEBIAN/control" <<EOF
Package: ${name}
Version: ${version}
Section: net
Priority: optional
Architecture: ${debian_arch}
Maintainer: Morrow contributors <morrow-project@users.noreply.github.com>
Depends: libc6 (>= 2.35), adduser
Description: WAL-backed message broker
 Morrow is a message broker with durable consumers, request/reply inboxes,
 and optional clustered durability.
EOF

cat >"${package_dir}/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e

if ! getent group morrow >/dev/null; then
    addgroup --system morrow
fi
if ! getent passwd morrow >/dev/null; then
    adduser --system --ingroup morrow --home /var/lib/morrow \
        --no-create-home --shell /usr/sbin/nologin morrow
fi

install -d -o morrow -g morrow -m 0750 /var/lib/morrow

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload || true
fi

exit 0
EOF
chmod 0755 "${package_dir}/DEBIAN/postinst"

mkdir -p "${output_dir}"
dpkg-deb --build --root-owner-group "${package_dir}" \
  "${output_dir}/${name}_${version}_${debian_arch}.deb"
