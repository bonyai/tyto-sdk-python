from __future__ import annotations

from typing import Optional

from .._http import HttpClient
from ..models import AuthPollResponse, AuthStartResponse


class AuthResource:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def start_cli(
        self,
        client: str = "tyto-cli",
        hostname: Optional[str] = None,
    ) -> AuthStartResponse:
        body: dict = {"client": client}
        if hostname is not None:
            body["hostname"] = hostname
        return AuthStartResponse.from_dict(self._http.post("/auth/cli/start", json=body))

    def poll_cli(self, device_code: str) -> AuthPollResponse:
        return AuthPollResponse.from_dict(
            self._http.post("/auth/cli/poll", json={"device_code": device_code})
        )
