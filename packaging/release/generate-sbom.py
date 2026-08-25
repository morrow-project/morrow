#!/usr/bin/env python3
"""Generate a small, deterministic SPDX SBOM for a Morrow release subject."""

import argparse
import hashlib
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--subject", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--target", required=True)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()

    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--format-version", "1", "--locked"],
            text=True,
        )
    )
    packages = []
    for package in sorted(metadata["packages"], key=lambda item: (item["name"], item["version"])):
        package_id = re.sub(r"[^A-Za-z0-9.-]+", "-", package["id"])
        packages.append(
            {
                "SPDXID": f"SPDXRef-Cargo-{package_id}",
                "name": package["name"],
                "versionInfo": package["version"],
                "downloadLocation": package.get("source") or "NOASSERTION",
                "licenseConcluded": "NOASSERTION",
                "licenseDeclared": "NOASSERTION",
                "filesAnalyzed": False,
                "externalRefs": [
                    {
                        "referenceCategory": "PACKAGE-MANAGER",
                        "referenceType": "purl",
                        "referenceLocator": f"pkg:cargo/{package['name']}@{package['version']}",
                    }
                ],
            }
        )

    subject = args.subject.resolve()
    subject_spdx = "SPDXRef-Subject"
    document = {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": subject.name,
        "documentNamespace": f"https://github.com/morrow-project/morrow/sbom/{args.version}/{subject.name}",
        "creationInfo": {
            "created": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "creators": ["Tool: morrow-sbom-generator"],
        },
        "packages": [
            {
                "SPDXID": subject_spdx,
                "name": subject.name,
                "versionInfo": args.version,
                "downloadLocation": "NOASSERTION",
                "licenseConcluded": "NOASSERTION",
                "licenseDeclared": "NOASSERTION",
                "filesAnalyzed": True,
                "files": [
                    {
                        "SPDXID": "SPDXRef-SubjectFile",
                        "fileName": subject.name,
                        "checksums": [{"algorithm": "SHA256", "checksumValue": sha256(subject)}],
                        "licenseConcluded": "NOASSERTION",
                    }
                ],
            },
            *packages,
        ],
        "relationships": [
            {
                "spdxElementId": "SPDXRef-DOCUMENT",
                "relationshipType": "DESCRIBES",
                "relatedSpdxElement": subject_spdx,
            }
        ],
        "morrow": {
            "targetTriple": args.target,
            "workspaceVersion": args.version,
            "subjectSha256": sha256(subject),
            "bundledThirdPartyCode": "Cargo dependency graph and the packaged subject are listed above.",
        },
    }
    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
