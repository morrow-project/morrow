#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 5 ]]; then
  echo "usage: $0 VERSION RPM_ARCH RELEASE_ARCH ARCHIVE OUTPUT_DIR" >&2
  exit 2
fi

version="$1"
rpm_arch="$2"
release_arch="$3"
archive="$4"
output_dir="$5"
build_root="$(mktemp -d)"
trap 'rm -rf "${build_root}"' EXIT
script_dir="$(cd "$(dirname "$0")" && pwd)"
unit_file="${script_dir}/../systemd/morrow.service"

if [[ ! -r "${unit_file}" ]]; then
  echo "missing shared systemd unit: ${unit_file}" >&2
  exit 1
fi

topdir="${build_root}/rpmbuild"
mkdir -p "${topdir}"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
cp "${archive}" "${topdir}/SOURCES/morrow-${version}-linux-${release_arch}.tar.gz"
cp "${unit_file}" "${topdir}/SOURCES/morrow.service"

cat >"${topdir}/SPECS/morrow.spec" <<EOF
Name:           morrow
Version:        ${version}
Release:        1%{?dist}
%global debug_package %{nil}
Summary:        WAL-backed message broker
License:        Apache-2.0
URL:            https://github.com/morrow-project/morrow
Source0:        morrow-%{version}-linux-${release_arch}.tar.gz
Source1:        morrow.service
Requires:       glibc >= 2.35

%description
Morrow is a message broker with durable consumers, request/reply inboxes,
and optional clustered durability.

%prep
%setup -q -n morrow-%{version}-linux-${release_arch}

%install
install -D -m 0755 morrow-server %{buildroot}%{_bindir}/morrow-server
install -D -m 0755 morrow-cli %{buildroot}%{_bindir}/morrow-cli
install -D -m 0755 morrow-connector %{buildroot}%{_bindir}/morrow-connector
install -D -m 0644 morrow.json.example %{buildroot}%{_sysconfdir}/morrow/morrow.json.example
install -D -m 0644 client.json.example %{buildroot}%{_docdir}/morrow/client.json.example
install -D -m 0644 LICENSE %{buildroot}%{_docdir}/morrow/LICENSE
install -D -m 0644 README.md %{buildroot}%{_docdir}/morrow/README.md
install -D -m 0644 docs/building.md %{buildroot}%{_docdir}/morrow/building.md
install -D -m 0644 docs/operations.md %{buildroot}%{_docdir}/morrow/operations.md
install -D -m 0644 docs/packaging-systemd.md %{buildroot}%{_docdir}/morrow/packaging-systemd.md
install -d -m 0750 %{buildroot}/var/lib/morrow
install -D -m 0644 %{SOURCE1} %{buildroot}%{_unitdir}/morrow.service

%pre
getent group morrow >/dev/null 2>&1 || groupadd -r morrow
getent passwd morrow >/dev/null 2>&1 || useradd -r -g morrow -d /var/lib/morrow -s /sbin/nologin -M morrow
exit 0

%post
install -d -o morrow -g morrow -m 0750 /var/lib/morrow
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload || :
fi
exit 0

%postun
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload || :
fi
exit 0

%files
%{_bindir}/morrow-server
%{_bindir}/morrow-cli
%{_bindir}/morrow-connector
%config(noreplace) %{_sysconfdir}/morrow/morrow.json.example
%{_unitdir}/morrow.service
%doc %{_docdir}/morrow/*
%dir /var/lib/morrow

%changelog
* Thu Jan 01 2026 Morrow contributors <morrow-project@users.noreply.github.com> - ${version}-1
- Package the Morrow ${version} Linux release archives.
EOF

mkdir -p "${output_dir}"
rpmbuild -bb \
  --define "_topdir ${topdir}" \
  --define "_unitdir /usr/lib/systemd/system" \
  --define "_build_id_links none" \
  --target "${rpm_arch}" \
  "${topdir}/SPECS/morrow.spec"

shopt -s nullglob
packages=("${topdir}/RPMS/${rpm_arch}/morrow-${version}-1"*."${rpm_arch}".rpm)
if [[ "${#packages[@]}" -ne 1 ]]; then
  echo "expected exactly one RPM package, found ${#packages[@]}" >&2
  printf '%s\n' "${packages[@]}" >&2
  exit 1
fi
cp "${packages[0]}" "${output_dir}/morrow-${version}-1.${rpm_arch}.rpm"
