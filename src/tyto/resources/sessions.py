from __future__ import annotations

from typing import Dict, List, Optional

from .._http import HttpClient
from ..config import TytoConfig
from ..models import SessionData
from ..ws import connect_ws


class Session:
    def __init__(self, data: SessionData, nest_id: str, http: HttpClient, config: TytoConfig) -> None:
        self._data = data
        self._nest_id = nest_id
        self._http = http
        self._config = config

    @property
    def id(self) -> Optional[int]:
        return self._data.id

    @property
    def nest_id(self) -> Optional[str]:
        return self._data.nest_id

    @property
    def tty(self) -> Optional[bool]:
        return self._data.tty

    @property
    def command(self) -> Optional[str]:
        return self._data.command

    @property
    def cwd(self) -> Optional[str]:
        return self._data.cwd

    @property
    def status(self) -> Optional[str]:
        return self._data.status

    @property
    def attached(self) -> Optional[int]:
        return self._data.attached

    @property
    def started_at(self) -> Optional[str]:
        return self._data.started_at

    @property
    def exit_code(self) -> Optional[int]:
        return self._data.exit_code

    @property
    def ended_at(self) -> Optional[str]:
        return self._data.ended_at

    @property
    def attach_url(self) -> Optional[str]:
        return self._data.attach_url

    @property
    def data(self) -> SessionData:
        return self._data

    def kill(self, signal: str = "TERM", grace_ms: int = 5000) -> "Session":
        result = self._http.post(
            f"/nest/{self._nest_id}/sessions/{self._data.id}/kill",
            json={"signal": signal, "grace_ms": grace_ms},
        )
        self._data = SessionData.from_dict(result)
        return self

    def attach(self):
        return connect_ws(self._config, f"/nest/{self._nest_id}/sessions/{self._data.id}/attach")


class SessionsResource:
    def __init__(self, nest_id: str, http: HttpClient, config: TytoConfig) -> None:
        self._nest_id = nest_id
        self._http = http
        self._config = config

    def create(
        self,
        argv: List[str],
        tty: bool = False,
        cwd: Optional[str] = None,
        cols: Optional[int] = None,
        rows: Optional[int] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> Session:
        body: dict = {"tty": tty, "argv": argv}
        if cwd is not None:
            body["cwd"] = cwd
        if cols is not None:
            body["cols"] = cols
        if rows is not None:
            body["rows"] = rows
        if env is not None:
            body["env"] = env
        result = self._http.post(f"/nest/{self._nest_id}/sessions", json=body)
        return Session(SessionData.from_dict(result), self._nest_id, self._http, self._config)

    def list(self, all: bool = False, history: bool = False) -> List[Session]:
        params: dict = {}
        if all:
            params["all"] = True
        if history:
            params["history"] = True
        result = self._http.get(f"/nest/{self._nest_id}/sessions", params=params)
        return [Session(SessionData.from_dict(d), self._nest_id, self._http, self._config) for d in result]
