from __future__ import annotations

from typing import List, Optional

from .._http import HttpClient
from ..models import KeepaliveHoldData


class HoldsResource:
    def __init__(self, nest_id: str, http: HttpClient) -> None:
        self._nest_id = nest_id
        self._http = http

    def list(self) -> List[KeepaliveHoldData]:
        result = self._http.get(f"/nest/{self._nest_id}/holds")
        return [KeepaliveHoldData.from_dict(d) for d in result]

    def put(
        self,
        name: str,
        ttl: Optional[str] = None,
        reason: Optional[str] = None,
        source: Optional[str] = None,
    ) -> KeepaliveHoldData:
        body: dict = {}
        if ttl is not None:
            body["ttl"] = ttl
        if reason is not None:
            body["reason"] = reason
        if source is not None:
            body["source"] = source
        result = self._http.put(f"/nest/{self._nest_id}/holds/{name}", json=body)
        return KeepaliveHoldData.from_dict(result)

    def delete(self, name: str) -> None:
        self._http.delete(f"/nest/{self._nest_id}/holds/{name}")

    def heartbeat(
        self,
        name: str,
        ttl: Optional[str] = None,
        reason: Optional[str] = None,
        source: Optional[str] = None,
    ) -> KeepaliveHoldData:
        body: dict = {}
        if ttl is not None:
            body["ttl"] = ttl
        if reason is not None:
            body["reason"] = reason
        if source is not None:
            body["source"] = source
        result = self._http.post(f"/nest/{self._nest_id}/holds/{name}/heartbeat", json=body)
        return KeepaliveHoldData.from_dict(result)
