# Release supply-chain verification

Tagged releases publish SHA-256 checksums, SPDX 2.3 SBOMs, keyless Sigstore
signatures, and GitHub artifact attestations for every archive and Debian/RPM
package. The multi-architecture image is signed by digest and receives an OCI
SBOM attachment; mutable `latest` and version tags are never used as verification
identities.

## Verify release files

Install [cosign](https://docs.sigstore.dev/cosign/system_config/installation/),
download the release assets, and verify a subject and its SBOM:

```sh
cosign verify-blob \
  --bundle morrow-0.1.1-linux-amd64.tar.gz.bundle \
  --certificate-identity-regexp \
  '^https://github.com/morrow-project/morrow/.github/workflows/tag-release.yml@refs/(heads/(main|maintain/.+)|pull/[0-9]+/merge)$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  morrow-0.1.1-linux-amd64.tar.gz
cosign verify-blob \
  --bundle morrow-0.1.1-linux-amd64.tar.gz.spdx.json.bundle \
  --certificate-identity-regexp \
  '^https://github.com/morrow-project/morrow/.github/workflows/tag-release.yml@refs/(heads/(main|maintain/.+)|pull/[0-9]+/merge)$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  morrow-0.1.1-linux-amd64.tar.gz.spdx.json
sha256sum --check morrow-0.1.1-linux-amd64.tar.gz.sha256
```

The same commands apply to `.deb` and `.rpm` subjects after replacing the
filename. GitHub's artifact-attestation UI/API provides an additional
verification path for the provenance attached to each published subject.

## Verify the container image

Resolve the image tag to its immutable digest, then verify the digest and its
SBOM attachment:

```sh
digest="$(docker buildx imagetools inspect ghcr.io/morrow-project/morrow-server:0.1.1 --format '{{json .Manifest.Digest}}' | tr -d '\"')"
cosign verify \
  --certificate-identity-regexp 'https://github.com/morrow-project/morrow/.github/workflows/tag-release.yml@refs/tags/0.1.1' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "ghcr.io/morrow-project/morrow-server@${digest}"
cosign download sbom "ghcr.io/morrow-project/morrow-server@${digest}" > image.spdx.json
```

The release workflow fails if the digest, signature, SBOM, or post-publish
verification is missing. It signs with GitHub's short-lived OIDC identity, so no
exportable signing key is stored in the repository or its secrets.

## Rotation and incident response

The identity is the reviewed workflow path and its protected release branch or
pull-request ref, while every signature is over an immutable subject digest and
the release jobs check out the created tag. The OIDC issuer is fixed to
Sigstore's GitHub Actions issuer. A workflow-path change is a deliberate
trust-boundary change: update the identity regex and this document in the same
reviewed pull request. For a suspected compromise, stop release
publishing, revoke or quarantine the affected GitHub release and container
digest, preserve workflow logs and attestation bundles, and publish a security
notice listing affected immutable digests. Rotate repository and package
permissions, review the workflow history, and issue a new tag only after the
verification job is green.
