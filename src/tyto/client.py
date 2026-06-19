from __future__ import annotations

from typing import Optional, Tuple

from ._http import HttpClient
from .config import TytoConfig, resolve_config
from .models import User
from .resources.auth import AuthResource
from .resources.nests import Nest, NestsResource
from .resources.previews import TopLevelPreviewsResource
from .resources.snapshots import TopLevelSnapshotsResource


def _parse_remote(remote: str) -> Tuple[str, str]:
    if ":" not in remote:
        raise ValueError(f"Invalid remote path {remote!r}: expected 'nestName:path'")
    nest_name, _, remote_path = remote.partition(":")
    return nest_name, remote_path


class Tyto:
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_url: Optional[str] = None,
    ) -> None:
        self._config = resolve_config(api_key=api_key, api_url=api_url)
        self._http = HttpClient(self._config)
        self.auth = AuthResource(self._http)
        self.nests = NestsResource(self._http, self._config)
        self.previews = TopLevelPreviewsResource(self._http)
        self.snapshots = TopLevelSnapshotsResource(self._http)

    def create(
        self,
        name: str,
        template: str = "ubuntu-24-dev",
        repo_url: Optional[str] = None,
    ) -> Nest:
        return self.nests.create(name=name, template=template, repo_url=repo_url)

    def put(self, local_path: str, remote: str) -> None:
        nest_name, remote_path = _parse_remote(remote)
        nest = self.nests.get_by_name(nest_name)
        nest.put(local_path, remote_path)

    def get(self, remote: str, local_path: str) -> None:
        nest_name, remote_path = _parse_remote(remote)
        nest = self.nests.get_by_name(nest_name)
        nest.get(remote_path, local_path)

    def health(self) -> dict:
        return self._http.get("/healthz")

    def ready(self) -> dict:
        return self._http.get("/readyz")

    def me(self) -> User:
        return User.from_dict(self._http.get("/me"))

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Tyto":
        return self

    def __exit__(self, *args) -> None:
        self.close()
