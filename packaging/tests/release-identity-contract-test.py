#!/usr/bin/env python3
"""Verify that release verification uses one least-privilege identity policy."""

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = ROOT / "packaging/release/cosign-policy.env"
DOC_PATH = ROOT / "docs/release-supply-chain.md"
WORKFLOW_PATH = ROOT / ".github/workflows/tag-release.yml"
VERIFY_SCRIPT_PATH = ROOT / "packaging/release/verify-release.sh"


def fail(message: str) -> None:
    print(f"release identity contract error: {message}", file=sys.stderr)
    raise SystemExit(1)


def read_policy() -> dict[str, str]:
    policy: dict[str, str] = {}
    for line in POLICY_PATH.read_text().splitlines():
        match = re.fullmatch(r"([A-Z][A-Z0-9_]*)='([^']+)'", line)
        if not match:
            fail(f"invalid policy line: {line!r}")
        policy[match.group(1)] = match.group(2)
    return policy


def documented_identities(documentation: str) -> list[str]:
    identities: list[str] = []
    marker = "--certificate-identity-regexp"
    offset = 0
    while (index := documentation.find(marker, offset)) != -1:
        window = documentation[index + len(marker) : index + len(marker) + 300]
        match = re.search(r"['\"](\^https://github\.com/[^'\"]+)['\"]", window)
        if not match:
            fail("could not parse a documented certificate identity")
        identities.append(match.group(1))
        offset = index + len(marker)
    return identities


def main() -> None:
    policy = read_policy()
    identity = policy.get("COSIGN_CERTIFICATE_IDENTITY_REGEXP")
    issuer = policy.get("COSIGN_CERTIFICATE_OIDC_ISSUER")
    if not identity or not issuer:
        fail("policy must define the identity regexp and OIDC issuer")

    compiled_identity = re.compile(identity)
    main_identity = (
        "https://github.com/morrow-project/morrow/.github/workflows/"
        "tag-release.yml@refs/heads/main"
    )
    maintenance_identity = main_identity.removesuffix("main") + "maintain/0.5"
    rejected_identities = [
        main_identity.removesuffix("refs/heads/main") + "refs/tags/0.5.2",
        main_identity.removesuffix("refs/heads/main") + "refs/pull/128/merge",
        main_identity.replace("morrow-project/morrow", "attacker/morrow"),
    ]
    if not compiled_identity.fullmatch(main_identity):
        fail("policy does not accept the observed main-branch release identity")
    if not compiled_identity.fullmatch(maintenance_identity):
        fail("policy does not accept supported maintenance release branches")
    if any(compiled_identity.fullmatch(candidate) for candidate in rejected_identities):
        fail("policy accepts a tag, pull-request merge ref, or another repository")

    documentation = DOC_PATH.read_text()
    identities = documented_identities(documentation)
    if not identities:
        fail("documentation contains no certificate verification commands")
    if any(documented != identity for documented in identities):
        fail("documentation certificate identity differs from the canonical policy")

    workflow = WORKFLOW_PATH.read_text()
    verify_script = VERIFY_SCRIPT_PATH.read_text()
    source_command = "source packaging/release/cosign-policy.env"
    if source_command not in workflow:
        fail("release workflow does not load the canonical policy")
    if "${COSIGN_CERTIFICATE_IDENTITY_REGEXP}" not in workflow:
        fail("release workflow does not use the canonical identity")
    if "${COSIGN_CERTIFICATE_OIDC_ISSUER}" not in workflow:
        fail("release workflow does not use the canonical issuer")
    if "packaging/release/cosign-policy.env" not in verify_script:
        fail("release asset verification does not load the canonical policy")
    if "${COSIGN_CERTIFICATE_IDENTITY_REGEXP}" not in verify_script:
        fail("release asset verification does not use the canonical identity")
    if "${COSIGN_CERTIFICATE_OIDC_ISSUER}" not in verify_script:
        fail("release asset verification does not use the canonical issuer")

    print(
        f"release identity contract is consistent across {len(identities)} "
        "documented commands and both verification paths"
    )


if __name__ == "__main__":
    main()
