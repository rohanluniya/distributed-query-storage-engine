"""Role-Based Access Control (RBAC), API key verification, and append-only audit logging."""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fastapi import Header, HTTPException, Request, status

ROLE_HIERARCHY: dict[str, int] = {
    "viewer": 1,
    "analyst": 2,
    "admin": 3,
}


@dataclass(frozen=True)
class Principal:
    role: str
    key_fingerprint: str


class SecurityManager:
    """Validates API keys against configured roles and records redacted audit events."""

    def __init__(self, audit_log_path: str | pathlib.Path) -> None:
        self.audit_log_path = pathlib.Path(audit_log_path)
        self.audit_log_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._key_to_role: dict[str, str] = {}
        self.reload_keys()

    def reload_keys(self) -> None:
        admin_key = os.environ.get("DQSE_API_KEY_ADMIN", "dev-admin-key-change-me")
        analyst_key = os.environ.get("DQSE_API_KEY_ANALYST", "dev-analyst-key-change-me")
        viewer_key = os.environ.get("DQSE_API_KEY_VIEWER", "dev-viewer-key-change-me")
        self._key_to_role = {
            admin_key: "admin",
            analyst_key: "analyst",
            viewer_key: "viewer",
        }

    def authenticate(self, api_key: str | None, min_role: str = "viewer") -> Principal:
        if not api_key or api_key not in self._key_to_role:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing or invalid X-API-Key header",
            )
        role = self._key_to_role[api_key]
        if ROLE_HIERARCHY[role] < ROLE_HIERARCHY[min_role]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{role}' lacks required privilege '{min_role}'",
            )
        fp = hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:12]
        return Principal(role=role, key_fingerprint=fp)

    def record_audit(
        self,
        action: str,
        principal: Principal | None,
        outcome: str,
        resource: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        event = {
            "timestamp_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "action": action,
            "role": principal.role if principal else "anonymous",
            "key_fingerprint": principal.key_fingerprint if principal else "none",
            "outcome": outcome,
            "resource": resource,
            "details": details or {},
        }
        line = json.dumps(event, default=str) + "\n"
        with self._lock, self.audit_log_path.open("a", encoding="utf-8") as fh:
            fh.write(line)

    def recent_audit_events(self, limit: int = 50) -> list[dict[str, Any]]:
        if not self.audit_log_path.exists():
            return []
        with self._lock:
            lines = self.audit_log_path.read_text(encoding="utf-8").splitlines()
        events = [json.loads(ln) for ln in lines[-limit:] if ln.strip()]
        return list(reversed(events))


def make_role_dependency(security: SecurityManager, min_role: str) -> Any:
    def _dep(
        request: Request,
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> Principal:
        try:
            principal = security.authenticate(x_api_key, min_role=min_role)
        except HTTPException as exc:
            security.record_audit(
                action=f"{request.method} {request.url.path}",
                principal=None,
                outcome=f"DENIED_{exc.status_code}",
                resource=request.url.path,
                details={"reason": exc.detail},
            )
            raise
        return principal

    return _dep
