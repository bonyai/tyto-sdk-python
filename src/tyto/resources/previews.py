from __future__ import annotations

from typing import List, Optional

from .._http import HttpClient
from ..models import PreviewData


class PreviewsResource:
    def __init__(self, nest_id: str, http: HttpClient) -> None:
        self._nest_id = nest_id
        self._http = http

    def create(
        self,
        port: int,
        auth: str = "private",
        public: bool = False,
        name: Optional[str] = None,
    ) -> PreviewData:
        body: dict = {"port": port, "auth": auth, "public": public}
        if name is not None:
            body["name"] = name
        result = self._http.post(f"/nest/{self._nest_id}/previews", json=body)
        return PreviewData.from_dict(result)

    def list(self) -> List[PreviewData]:
        result = self._http.get(f"/nest/{self._nest_id}/previews")
        return [PreviewData.from_dict(d) for d in result]


class TopLevelPreviewsResource:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def get(self, preview_id: str) -> PreviewData:
        return PreviewData.from_dict(self._http.get(f"/previews/{preview_id}"))

    def revoke(self, preview_id: str) -> None:
        self._http.delete(f"/previews/{preview_id}")
