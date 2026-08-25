#!/usr/bin/env bash
set -euo pipefail

dist_dir="${1:?usage: verify-release.sh DIST VERSION}"
version="${2:?usage: verify-release.sh DIST VERSION}"
identity_regex='^https://github.com/morrow-project/morrow/.github/workflows/tag-release.yml@refs/(heads/(main|maintain/.+)|pull/[0-9]+/merge)$'
issuer="https://token.actions.githubusercontent.com"

shopt -s nullglob
subjects=("${dist_dir}"/*.tar.gz "${dist_dir}"/*.deb "${dist_dir}"/*.rpm)
if ((${#subjects[@]} == 0)); then
  echo "no release subjects found" >&2
  exit 1
fi

for subject in "${subjects[@]}"; do
  test -s "${subject}.spdx.json"
  test -s "${subject}.sig"
  test -s "${subject}.bundle"
  test -s "${subject}.spdx.json.sig"
  test -s "${subject}.spdx.json.bundle"
  cosign verify-blob --bundle "${subject}.bundle" \
    --certificate-identity-regexp "${identity_regex}" \
    --certificate-oidc-issuer "${issuer}" "${subject}"
  cosign verify-blob --bundle "${subject}.spdx.json.bundle" \
    --certificate-identity-regexp "${identity_regex}" \
    --certificate-oidc-issuer "${issuer}" "${subject}.spdx.json"
done

for checksum in "${dist_dir}"/*.tar.gz.sha256 "${dist_dir}"/*.deb.sha256 "${dist_dir}"/*.rpm.sha256; do
  ((${#checksum} > 0)) || continue
  (cd "${dist_dir}" && sha256sum --check "$(basename "${checksum}")")
done
