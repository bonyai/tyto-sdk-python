from __future__ import annotations

import io
import os
import shlex
import tarfile
import time
import uuid
from pathlib import Path
from typing import List, Optional, Union

from .._http import HttpClient
from ..config import TytoConfig
from ..models import (
    DeleteSnapshotResponse,
    ForkResponse,
    NestData,
    NestLifecycle,
    PreviewData,
    RestoreResponse,
    SessionData,
    SnapshotData,
    WakeResponse,
)
from ..ws import connect_ws
from .files import FileSystem
from .holds import HoldsResource
from .previews import PreviewsResource
from .sessions import Session, SessionsResource
from .snapshots import SnapshotsResource


class Nest:
    def __init__(self, data: NestData, http: HttpClient, config: TytoConfig) -> None:
        self._data = data
        self._http = http
        self._config = config
        self.fs = FileSystem(data.id, http)
        self.sessions = SessionsResource(data.id, http, config)
        self.previews = PreviewsResource(data.id, http)
        self.snapshots = SnapshotsResource(data.id, http)
        self.holds = HoldsResource(data.id, http)

    @property
    def id(self) -> str:
        return self._data.id

    @property
    def user_id(self) -> str:
        return self._data.user_id

    @property
    def name(self) -> str:
        return self._data.name

    @property
    def template(self) -> str:
        return self._data.template

    @property
    def status(self) -> str:
        return self._data.status

    @property
    def repo_url(self) -> Optional[str]:
        return self._data.repo_url

    @property
    def error_message(self) -> Optional[str]:
        return self._data.error_message

    @property
    def sleep_source(self) -> Optional[str]:
        return self._data.sleep_source

    @property
    def lifecycle_error(self) -> Optional[str]:
        return self._data.lifecycle_error

    @property
    def last_activity_at(self) -> Optional[str]:
        return self._data.last_activity_at

    @property
    def last_wake_at(self) -> Optional[str]:
        return self._data.last_wake_at

    @property
    def created_at(self) -> str:
        return self._data.created_at

    @property
    def updated_at(self) -> str:
        return self._data.updated_at

    @property
    def data(self) -> NestData:
        return self._data

    def start(self) -> Union["Nest", WakeResponse]:
        result = self._http.post(f"/nest/{self._data.id}/start")
        if "user_id" in result:
            self._data = NestData.from_dict(result)
            return self
        return WakeResponse.from_dict(result)

    def stop(self) -> "Nest":
        result = self._http.post(f"/nest/{self._data.id}/stop")
        self._data = NestData.from_dict(result)
        return self

    def wake(self, reason: Optional[str] = None) -> WakeResponse:
        body = {"reason": reason} if reason else {}
        return WakeResponse.from_dict(self._http.post(f"/nest/{self._data.id}/wake", json=body))

    def delete(self) -> Optional[NestData]:
        result = self._http.delete(f"/nest/{self._data.id}")
        return NestData.from_dict(result) if result else None

    def lifecycle(self) -> NestLifecycle:
        return NestLifecycle.from_dict(self._http.get(f"/nest/{self._data.id}/lifecycle"))

    def restore(self, snapshot_id: str) -> RestoreResponse:
        result = self._http.post(
            f"/nest/{self._data.id}/restore",
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
            f"/nest/{self._data.id}/fork",
            json={"name": name, "stop_if_running": stop_if_running, "restart_source": restart_source},
        )
        return ForkResponse.from_dict(result)

    def run(
        self,
        argv: list,
        cwd: Optional[str] = None,
        cols: int = 80,
        rows: int = 24,
        timeout_s: float = 120.0,
    ) -> str:
        """Run a command in the nest and return its captured output.

        The API runs managed sessions to completion server-side and only exposes
        live output over a WebSocket, which is racy for short commands (the
        session can finish before the stream attaches, yielding HTTP 410). So we
        redirect the command's stdout+stderr and exit code to files, poll the
        filesystem until the exit-code file appears, then read the output back
        over the (reliable) filesystem API. No WebSocket required.
        """
        work_dir = "/home/tyto/.tyto-run"
        run_id = uuid.uuid4().hex
        out_abs = f"{work_dir}/{run_id}.out"
        rc_abs = f"{work_dir}/{run_id}.rc"
        out_rel = f".tyto-run/{run_id}.out"
        rc_rel = f".tyto-run/{run_id}.rc"

        effective_cwd = cwd or "/home/tyto"
        inner = " ".join(shlex.quote(a) for a in argv)
        script = (
            f"mkdir -p {shlex.quote(work_dir)}; "
            f"{{ cd {shlex.quote(effective_cwd)} && {inner} ; }} > {shlex.quote(out_abs)} 2>&1; "
            f"echo $? > {shlex.quote(rc_abs)}"
        )

        self.sessions.create(
            argv=["bash", "-lc", script], tty=True, cwd=cwd, cols=cols, rows=rows
        )

        # The exit-code file is written only after the command completes, so its
        # presence is an unambiguous "done" signal. (Session status is unreliable
        # here: a quiet tty session reports "idle" while still running.)
        deadline = time.perf_counter() + timeout_s
        while time.perf_counter() < deadline:
            time.sleep(0.5)
            try:
                if self.fs.read(rc_rel).data.decode().strip():
                    break
            except Exception:
                continue  # exit-code file not written yet

        try:
            return self.fs.read(out_rel).data.decode(errors="replace")
        except Exception:
            return ""

    def create_snapshot(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        stop_if_running: bool = False,
    ) -> SnapshotData:
        return self.snapshots.create(name=name, description=description, stop_if_running=stop_if_running)

    def delete_snapshot(self, snapshot_id: str, dry_run: bool = False) -> DeleteSnapshotResponse:
        params: dict = {"dry_run": dry_run} if dry_run else {}
        result = self._http.delete(f"/snapshots/{snapshot_id}", params=params)
        return DeleteSnapshotResponse.from_dict(result)

    def create_session(
        self,
        argv: list,
        tty: bool = False,
        cwd: Optional[str] = None,
        cols: Optional[int] = None,
        rows: Optional[int] = None,
        env: Optional[dict] = None,
    ) -> Session:
        return self.sessions.create(argv=argv, tty=tty, cwd=cwd, cols=cols, rows=rows, env=env)

    def create_preview(
        self,
        port: int,
        auth: str = "private",
        public: bool = False,
        name: Optional[str] = None,
    ) -> PreviewData:
        return self.previews.create(port=port, auth=auth, public=public, name=name)

    def put(self, local_path: str, remote_path: str) -> None:
        local = Path(local_path)
        if local.is_dir():
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w:") as tf:
                tf.add(str(local), arcname=".")
            self.fs.write(remote_path, buf.getvalue(), kind="dir")
        else:
            self.fs.write(remote_path, local.read_bytes(), kind="file")

    def get(self, remote_path: str, local_path: str) -> None:
        result = self.fs.read(remote_path)
        if result.kind == "dir":
            dest = Path(local_path)
            dest.mkdir(parents=True, exist_ok=True)
            buf = io.BytesIO(result.data)
            with tarfile.open(fileobj=buf, mode="r:") as tf:
                tf.extractall(str(dest))
        else:
            dest = Path(local_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(result.data)

    def console(self):
        return connect_ws(self._config, f"/nest/{self._data.id}/console")

    def exec(self):
        return connect_ws(self._config, f"/nest/{self._data.id}/exec")

    def __repr__(self) -> str:
        return f"Nest(id={self.id!r}, name={self.name!r}, status={self.status!r})"


class NestsResource:
    def __init__(self, http: HttpClient, config: TytoConfig) -> None:
        self._http = http
        self._config = config

    def create(
        self,
        name: str,
        template: str = "ubuntu-24-dev",
        repo_url: Optional[str] = None,
    ) -> Nest:
        body: dict = {"name": name, "template": template}
        if repo_url is not None:
            body["repo_url"] = repo_url
        result = self._http.post("/nest/", json=body)
        return Nest(NestData.from_dict(result), self._http, self._config)

    def list(self) -> List[Nest]:
        result = self._http.get("/nest/")
        return [Nest(NestData.from_dict(d), self._http, self._config) for d in result]

    def get(self, nest_id: str) -> Nest:
        result = self._http.get(f"/nest/{nest_id}")
        return Nest(NestData.from_dict(result), self._http, self._config)

