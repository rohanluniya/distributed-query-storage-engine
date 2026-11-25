"""Audit installed Python dependency licenses against permissive open-source policy."""

from __future__ import annotations

import importlib.metadata
import json
import pathlib
import sys
from datetime import UTC, datetime

ALLOWED_TOKENS = (
    "apache",
    "mit",
    "bsd",
    "isc",
    "psf",
    "python software foundation",
    "mpl",
    "mozilla",
    "lgpl",
    "0bsd",
    "unlicense",
    "cc0",
    "pil",
    "hpnd",
    "historical permission",
    "zpl",
)
DISALLOWED_TOKENS = ("gpl-3", "gplv3", "agpl", "sspl", "busl", "proprietary", "commercial only")


def _extract_license(meta: importlib.metadata.PackageMetadata) -> str:
    lic_expr = meta.get("License-Expression")
    if lic_expr:
        return lic_expr.strip()
    classifiers = [
        c.split(" :: ")[-1].strip()
        for c in (meta.get_all("Classifier") or [])
        if c.startswith("License ::")
    ]
    if classifiers:
        return "; ".join(classifiers)
    lic = meta.get("License")
    if lic and lic.strip().upper() != "UNKNOWN":
        return lic.strip().splitlines()[0][:120]
    return "UNKNOWN"


def main() -> None:
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    out_path = repo_root / "results" / "licenses" / "license_report.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    packages = []
    violations = []
    for dist in sorted(importlib.metadata.distributions(), key=lambda d: d.metadata["Name"].lower()):
        name = dist.metadata["Name"]
        version = dist.version
        lic = _extract_license(dist.metadata)
        low = lic.lower()
        is_disallowed = any(tok in low for tok in DISALLOWED_TOKENS)
        is_known_ok = any(tok in low for tok in ALLOWED_TOKENS) and not is_disallowed
        entry = {
            "package": name,
            "version": version,
            "license": lic,
            "status": "violation" if is_disallowed else ("approved" if is_known_ok else "review"),
        }
        packages.append(entry)
        if is_disallowed:
            violations.append(entry)

    report = {
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "total_packages": len(packages),
        "violations": violations,
        "dataset_license_compliance": {
            "dataset": "clean-room synthetic order-analytics (orders, lineitem, customer, nation)",
            "source_policy": "Original Apache-2.0 generator inspired by TPC-H schema shapes; does not embed TPC-H code/data or claim official audited TPC-H compliance.",
            "territorial_or_commercial_restrictions": "None (Apache-2.0)",
        },
        "packages": packages,
    }
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Audited {len(packages)} packages -> {out_path} (violations={len(violations)})")
    if violations:
        sys.exit(1)


if __name__ == "__main__":
    main()
