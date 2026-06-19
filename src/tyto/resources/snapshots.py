from __future__ import annotations

from typing import Optional

from .._http import HttpClient
from ..models import (
    DeleteSnapshotResponse,
    ForkResponse,
    RestoreResponse,
    SnapshotData,
    SnapshotList,
)


class SnapshotsResource:
    def __init__(self, nest_id: str, http: HttpClient) -> None:
        self._nest_id = nest_id
        self._http = http

    def create(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        stop_if_running: bool = False,
    ) -> SnapshotData:
        body: dict = {"stop_if_running": stop_if_running}
        if name is not None:
            body["name"] = name
        if description is not None:
            body["description"] = description
        result = self._http.post(f"/nest/{self._nest_id}/snapshots", json=body)
        return SnapshotData.from_dict(result)

    def list(self) -> SnapshotList:
        return SnapshotList.from_dict(self._http.get(f"/nest/{self._nest_id}/snapshots"))

    def restore(self, snapshot_id: str) -> RestoreResponse:
        result = self._http.post(
            f"/nest/{self._nest_id}/restore",
            json={"snapshot_id": snapshot_id},
        )
        return RestoreResponse.from_dict(result)

    def fork(
        self,
        name: str,
        stop_if_running: bool = False,
        restart_source: bool = False,
    ) -> ForkResponse:
        result = self._http.post(
            f"/nest/{self._nest_id}/fork",
            json={"name": name, "stop_if_running": stop_if_running, "restart_source": restart_source},
        )
        return ForkResponse.from_dict(result)


class TopLevelSnapshotsResource:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def delete(self, snapshot_id: str, dry_run: bool = False) -> DeleteSnapshotResponse:
        params = {"dry_run": dry_run} if dry_run else {}
        result = self._http.delete(f"/snapshots/{snapshot_id}", params=params)
        return DeleteSnapshotResponse.from_dict(result)
