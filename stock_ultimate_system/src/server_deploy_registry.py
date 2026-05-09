from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.utils.project_paths import resolve_project_path


SERVER_DEPLOY_SNAPSHOT_VERSION = "server_deploy_snapshot.v1"
SERVER_DEPLOY_CURRENT_POINTER_VERSION = "server_deploy_current_pointer.v1"
SUPPORTED_DEPLOY_SCOPES = ("stock-scoped", "full-domain")


def _required_route_scopes(scope: str) -> tuple[str, ...]:
    if scope == "stock-scoped":
        return ("stock",)
    if scope == "full-domain":
        return ("main_site", "stock", "t12")
    raise ValueError(f"unsupported server deploy scope: {scope}")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normalize_deployment_id(value: str) -> str:
    deployment_id = str(value).strip()
    if not deployment_id:
        raise ValueError("deployment_id must not be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if any(ch not in allowed for ch in deployment_id):
        raise ValueError("deployment_id must contain only letters, numbers, '-' or '_'")
    return deployment_id


@dataclass(frozen=True)
class ServerDeploySnapshot:
    deployment_id: str
    scope: str
    registered_at: str
    source_evidence_bundle_path: str
    evidence_bundle_hash: str
    route_topology: dict[str, object]
    activation_execution: dict[str, object]
    rollback_available: bool
    snapshot_version: str = SERVER_DEPLOY_SNAPSHOT_VERSION

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshot_version": self.snapshot_version,
            "deployment_id": self.deployment_id,
            "scope": self.scope,
            "registered_at": self.registered_at,
            "source_evidence_bundle_path": self.source_evidence_bundle_path,
            "evidence_bundle_hash": self.evidence_bundle_hash,
            "route_topology": self.route_topology,
            "activation_execution": self.activation_execution,
            "rollback_available": self.rollback_available,
        }


class ServerDeployRegistry:
    def __init__(self, *, deployments_dir: str | Path = "/opt/stock-ultimate/deployments") -> None:
        self.deployments_dir = resolve_project_path(deployments_dir)
        self.history_dir = self.deployments_dir / "history"
        self.current_path = self.deployments_dir / "current.json"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.deployments_dir.mkdir(parents=True, exist_ok=True)
        self.history_dir.mkdir(parents=True, exist_ok=True)
        if not self.current_path.exists():
            _write_json(
                self.current_path,
                {
                    "pointer_version": SERVER_DEPLOY_CURRENT_POINTER_VERSION,
                    "deployment_id": None,
                    "scope": None,
                    "snapshot_path": None,
                    "updated_at": None,
                    "rollback_of_deployment_id": None,
                },
            )

    def get_current_pointer(self) -> dict[str, object]:
        return _read_json(self.current_path)

    def get_current_snapshot(self) -> dict[str, object] | None:
        pointer = self.get_current_pointer()
        snapshot_path = pointer.get("snapshot_path")
        if not snapshot_path:
            return None
        return _read_json(Path(str(snapshot_path)))

    def get_snapshot(self, deployment_id: str) -> dict[str, object]:
        resolved_deployment_id = _normalize_deployment_id(deployment_id)
        snapshot_path = self.history_dir / f"{resolved_deployment_id}.json"
        if not snapshot_path.exists():
            raise FileNotFoundError(f"server deployment snapshot not found: {snapshot_path}")
        snapshot = _read_json(snapshot_path)
        snapshot["_snapshot_path"] = str(snapshot_path)
        return snapshot

    def register(
        self,
        *,
        evidence_bundle_path: str | Path,
        deployment_id: str | None = None,
        registered_at: str | None = None,
    ) -> dict[str, object]:
        bundle_path = resolve_project_path(evidence_bundle_path)
        self._validate_evidence_bundle(bundle_path)
        bundle_payload = _read_json(bundle_path)
        resolved_deployment_id = _normalize_deployment_id(
            deployment_id or str(bundle_payload.get("deployment_id", "") or "")
        )
        snapshot_path = self.history_dir / f"{resolved_deployment_id}.json"
        if snapshot_path.exists():
            raise FileExistsError(f"server deployment snapshot already exists: {snapshot_path}")

        snapshot = ServerDeploySnapshot(
            deployment_id=resolved_deployment_id,
            scope=str(bundle_payload.get("scope") or ""),
            registered_at=registered_at or _utc_now_iso(),
            source_evidence_bundle_path=str(bundle_path),
            evidence_bundle_hash=_sha256_file(bundle_path),
            route_topology=dict(bundle_payload.get("route_topology", {}) or {}),
            activation_execution=dict(bundle_payload.get("activation_execution", {}) or {}),
            rollback_available=bool((bundle_payload.get("rollback") or {}).get("available")),
        )
        _write_json(snapshot_path, snapshot.as_dict())
        self._write_current_pointer(
            deployment_id=resolved_deployment_id,
            scope=snapshot.scope,
            snapshot_path=snapshot_path,
            rollback_of_deployment_id=None,
        )
        return snapshot.as_dict()

    def rollback(self, deployment_id: str, *, rolled_back_at: str | None = None) -> dict[str, object]:
        resolved_deployment_id = _normalize_deployment_id(deployment_id)
        snapshot_path = self.history_dir / f"{resolved_deployment_id}.json"
        if not snapshot_path.exists():
            raise FileNotFoundError(f"server deployment snapshot not found: {snapshot_path}")
        snapshot = _read_json(snapshot_path)
        self._write_current_pointer(
            deployment_id=resolved_deployment_id,
            scope=str(snapshot.get("scope") or ""),
            snapshot_path=snapshot_path,
            rollback_of_deployment_id=resolved_deployment_id,
            updated_at=rolled_back_at or _utc_now_iso(),
        )
        return snapshot

    def _write_current_pointer(
        self,
        *,
        deployment_id: str,
        scope: str,
        snapshot_path: Path,
        rollback_of_deployment_id: str | None,
        updated_at: str | None = None,
    ) -> None:
        _write_json(
            self.current_path,
            {
                "pointer_version": SERVER_DEPLOY_CURRENT_POINTER_VERSION,
                "deployment_id": deployment_id,
                "scope": scope,
                "snapshot_path": str(snapshot_path),
                "updated_at": updated_at or _utc_now_iso(),
                "rollback_of_deployment_id": rollback_of_deployment_id,
            },
        )

    def _validate_evidence_bundle(self, bundle_path: Path) -> None:
        if not bundle_path.exists():
            raise FileNotFoundError(f"server deploy evidence bundle missing: {bundle_path}")
        bundle_payload = _read_json(bundle_path)
        if bundle_payload.get("server_deploy_evidence_bundle_version") != "server_deploy_evidence_bundle.v1":
            raise ValueError("server deploy evidence bundle version is invalid")
        if bundle_payload.get("status") != "passed":
            raise ValueError("server deploy evidence bundle must be passed")
        deployment_id = str(bundle_payload.get("deployment_id", "") or "").strip()
        _normalize_deployment_id(deployment_id)
        scope = str(bundle_payload.get("scope") or "").strip()
        if scope not in SUPPORTED_DEPLOY_SCOPES:
            raise ValueError(f"server deploy evidence bundle scope is invalid: {scope or '<missing>'}")
        activation_execution = bundle_payload.get("activation_execution")
        if not isinstance(activation_execution, dict):
            raise ValueError("server deploy evidence bundle missing activation_execution")
        if activation_execution.get("action") != "activate":
            raise ValueError("server deploy evidence bundle activation_execution must be activate")
        if activation_execution.get("dry_run") is not False:
            raise ValueError("server deploy evidence bundle activation_execution must not be dry-run")
        if activation_execution.get("failed_total") != 0:
            raise ValueError("server deploy evidence bundle activation_execution failed_total must be zero")
        rollback = bundle_payload.get("rollback")
        if not isinstance(rollback, dict) or rollback.get("available") is not True:
            raise ValueError("server deploy evidence bundle must include available rollback")
        route_topology = bundle_payload.get("route_topology")
        if not isinstance(route_topology, dict):
            raise ValueError("server deploy evidence bundle missing route_topology")
        for scope_id in _required_route_scopes(scope):
            if scope_id not in route_topology:
                raise ValueError(f"server deploy evidence bundle missing route_topology.{scope_id}")
