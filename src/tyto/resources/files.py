from __future__ import annotations

from .._http import HttpClient
from ..models import ReadFileResult


class FileSystem:
    def __init__(self, nest_id: str, http: HttpClient) -> None:
        self._nest_id = nest_id
        self._http = http

    def write(self, path: str, data: bytes, kind: str = "file") -> None:
        content_type = "application/x-tar" if kind == "dir" else "application/octet-stream"
        self._http.put_binary(
            f"/nest/{self._nest_id}/fs/write",
            data=data,
            content_type=content_type,
            params={"path": path, "kind": kind},
        )

    def read(self, path: str) -> ReadFileResult:
        data, kind = self._http.get_binary(
            f"/nest/{self._nest_id}/fs/read",
            params={"path": path},
        )
        return ReadFileResult(data=data, kind=kind)
