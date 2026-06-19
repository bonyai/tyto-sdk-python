from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type, TypeVar

T = TypeVar("T")


def _from_dict(cls: Type[T], d: dict) -> T:
    known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
    return cls(**{k: v for k, v in d.items() if k in known})  # type: ignore[call-arg]


@dataclass
class User:
    id: str
    email: str

    @classmethod
    def from_dict(cls, d: dict) -> "User":
        return _from_dict(cls, d)


@dataclass
class AuthStartResponse:
    login_url: str
    device_code: str
    expires_in: int

    @classmethod
    def from_dict(cls, d: dict) -> "AuthStartResponse":
        return _from_dict(cls, d)


@dataclass
class AuthPollResponse:
    status: str
    api_key: Optional[str] = None
    user: Optional[User] = None

    @classmethod
    def from_dict(cls, d: dict) -> "AuthPollResponse":
        obj = _from_dict(cls, d)
        if isinstance(obj.user, dict):
            obj.user = User.from_dict(obj.user)
        return obj


@dataclass
class NestData:
    id: str
    user_id: str
    name: str
    template: str
    status: str
    created_at: str
    updated_at: str
    repo_url: Optional[str] = None
    error_message: Optional[str] = None
    sleep_source: Optional[str] = None
    lifecycle_error: Optional[str] = None
    last_activity_at: Optional[str] = None
    last_wake_at: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "NestData":
        return _from_dict(cls, d)


@dataclass
class WakeResponse:
    nest_id: Optional[str] = None
    from_: Optional[str] = None
    to: Optional[str] = None
    path: Optional[str] = None
    reason: Optional[str] = None
    duration_ms: Optional[int] = None

    @classmethod
    def from_dict(cls, d: dict) -> "WakeResponse":
        mapped = {("from_" if k == "from" else k): v for k, v in d.items()}
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in mapped.items() if k in known})


@dataclass
class NestLifecycle:
    nest_id: Optional[str] = None
    status: Optional[str] = None
    sleep_source: Optional[str] = None
    last_activity_at: Optional[str] = None
    last_wake_at: Optional[str] = None
    lifecycle_error: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "NestLifecycle":
        return _from_dict(cls, d)


@dataclass
class SessionData:
    id: Optional[int] = None
    nest_id: Optional[str] = None
    tty: Optional[bool] = None
    command: Optional[str] = None
    cwd: Optional[str] = None
    status: Optional[str] = None
    attached: Optional[int] = None
    started_at: Optional[str] = None
    last_activity_at: Optional[str] = None
    exit_code: Optional[int] = None
    ended_at: Optional[str] = None
    attach_url: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "SessionData":
        return _from_dict(cls, d)


@dataclass
class PreviewData:
    id: Optional[str] = None
    nest_id: Optional[str] = None
    name: Optional[str] = None
    port: Optional[int] = None
    auth: Optional[str] = None
    public: Optional[bool] = None
    url: Optional[str] = None
    path_url: Optional[str] = None
    created_at: Optional[str] = None
    expires_at: Optional[str] = None
    revoked_at: Optional[str] = None
    expires_in: Optional[int] = None

    @classmethod
    def from_dict(cls, d: dict) -> "PreviewData":
        return _from_dict(cls, d)


@dataclass
class SnapshotData:
    id: Optional[str] = None
    nest_id: Optional[str] = None
    user_id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    state: Optional[str] = None
    template_id: Optional[str] = None
    logical_dirty_bytes: Optional[int] = None
    apparent_bytes: Optional[int] = None
    physical_bytes: Optional[int] = None
    reclaimable_bytes: Optional[int] = None
    reclaimable_status: Optional[str] = None
    index_status: Optional[str] = None
    created_at: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "SnapshotData":
        return _from_dict(cls, d)


@dataclass
class SnapshotList:
    nest_id: Optional[str] = None
    snapshots: Optional[List[SnapshotData]] = None

    @classmethod
    def from_dict(cls, d: dict) -> "SnapshotList":
        obj = _from_dict(cls, d)
        if obj.snapshots:
            obj.snapshots = [SnapshotData.from_dict(s) if isinstance(s, dict) else s for s in obj.snapshots]
        return obj


@dataclass
class RestoreResponse:
    nest_id: Optional[str] = None
    restored_from: Optional[str] = None
    status: Optional[str] = None
    message: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "RestoreResponse":
        return _from_dict(cls, d)


@dataclass
class ForkStorage:
    copy_method: Optional[str] = None
    physical_bytes_added_now: Optional[int] = None

    @classmethod
    def from_dict(cls, d: dict) -> "ForkStorage":
        return _from_dict(cls, d)


@dataclass
class ForkResponse:
    id: Optional[str] = None
    name: Optional[str] = None
    source_nest_id: Optional[str] = None
    status: Optional[str] = None
    template_id: Optional[str] = None
    source_restarted: Optional[bool] = None
    source_restart_error: Optional[str] = None
    storage: Optional[ForkStorage] = None

    @classmethod
    def from_dict(cls, d: dict) -> "ForkResponse":
        obj = _from_dict(cls, d)
        if isinstance(obj.storage, dict):
            obj.storage = ForkStorage.from_dict(obj.storage)
        return obj


@dataclass
class DeleteSnapshotResponse:
    snapshot_id: Optional[str] = None
    can_delete: Optional[bool] = None
    would_free_bytes: Optional[int] = None
    would_remain_shared_bytes: Optional[int] = None
    reclaimable_status: Optional[str] = None
    blocked_by: Optional[List[str]] = None
    deleted: Optional[bool] = None

    @classmethod
    def from_dict(cls, d: dict) -> "DeleteSnapshotResponse":
        return _from_dict(cls, d)


@dataclass
class KeepaliveHoldData:
    nest_id: Optional[str] = None
    name: Optional[str] = None
    source: Optional[str] = None
    reason: Optional[str] = None
    expires_at: Optional[str] = None
    last_heartbeat_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "KeepaliveHoldData":
        return _from_dict(cls, d)


@dataclass
class ReadFileResult:
    data: bytes
    kind: str
