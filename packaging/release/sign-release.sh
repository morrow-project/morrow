#!/usr/bin/env bash
set -euo pipefail

dist_dir="${1:?usage: sign-release.sh DIST VERSION TARGET}"
version="${2:?usage: sign-release.sh DIST VERSION TARGET}"
target="${3:?usage: sign-release.sh DIST VERSION TARGET}"
repo_root="$(cd "$(dirname "$0")/../.." && pwd)"

mkdir -p "${dist_dir}"
shopt -s nullglob
subjects=("${dist_dir}"/*.tar.gz "${dist_dir}"/*.deb "${dist_dir}"/*.rpm)
if ((${#subjects[@]} == 0)); then
  echo "no release archives or packages found in ${dist_dir}" >&2
  exit 1
fi

for subject in "${subjects[@]}"; do
  case "${subject}" in
    *linux-amd64*|*_amd64.deb|*-1.x86_64.rpm)
      subject_target="x86_64-unknown-linux-gnu"
      ;;
    *linux-arm64*|*_arm64.deb|*-1.aarch64.rpm)
      subject_target="aarch64-unknown-linux-gnu"
      ;;
    *macos-arm64*)
      subject_target="aarch64-apple-darwin"
      ;;
    *)
      subject_target="release-artifact"
      ;;
  esac
  sbom="${subject}.spdx.json"
  python3 "${repo_root}/packaging/release/generate-sbom.py" \
    --subject "${subject}" \
    --output "${sbom}" \
    --target "${subject_target:-${target}}" \
    --version "${version}"
  cosign sign-blob --yes --bundle "${subject}.bundle" \
    --output-signature "${subject}.sig" "${subject}"
  cosign sign-blob --yes --bundle "${sbom}.bundle" \
    --output-signature "${sbom}.sig" "${sbom}"
  (
    cd "$(dirname "${subject}")"
    sha256sum "$(basename "${subject}")"
  ) > "${subject}.sha256"
done
