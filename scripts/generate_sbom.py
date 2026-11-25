"""Generate CycloneDX 1.5 JSON Software Bill of Materials (SBOM) for installed Python components."""

from __future__ import annotations

import importlib.metadata
import json
import pathlib
import uuid
from datetime import UTC, datetime


def main() -> None:
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    out_path = repo_root / "results" / "sbom" / "sbom.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    components = []
    for dist in sorted(importlib.metadata.distributions(), key=lambda d: d.metadata["Name"].lower()):
        name = dist.metadata["Name"]
        version = dist.version
        purl = f"pkg:pypi/{name.lower()}@{version}"
        components.append(
            {
                "type": "library",
                "name": name,
                "version": version,
                "purl": purl,
            }
        )

    sbom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:{uuid.uuid4()}",
        "version": 1,
        "metadata": {
            "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "component": {
                "type": "application",
                "name": "distributed-query-storage-engine",
                "version": "1.0.0",
                "licenses": [{"license": {"id": "Apache-2.0"}}],
            },
        },
        "components": components,
    }
    out_path.write_text(json.dumps(sbom, indent=2), encoding="utf-8")
    print(f"Generated CycloneDX SBOM with {len(components)} components -> {out_path}")


if __name__ == "__main__":
    main()
