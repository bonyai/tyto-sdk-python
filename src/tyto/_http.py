from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from .config import TytoConfig
from .errors import TytoAPIError


class HttpClient:
    def __init__(self, config: TytoConfig) -> None:
        self._config = config
        self._client = httpx.Client(
            base_url=config.api_url,
            headers={"Authorization": f"Bearer {config.api_key}"},
            timeout=30.0,
        )

    def _raise_for_status(self, response: httpx.Response) -> None:
        if response.is_success:
            return
        try:
            body = response.json()
            code = body.get("error")
            message = body.get("message") or f"HTTP {response.status_code}"
        except Exception:
            code = None
            message = f"HTTP {response.status_code}"
        raise TytoAPIError(response.status_code, code, message)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        resp = self._client.get(path, params={k: v for k, v in (params or {}).items() if v is not None})
        self._raise_for_status(resp)
        if resp.status_code == 204:
            return None
        return resp.json()

    def post(self, path: str, json: Any = None, params: Optional[Dict[str, Any]] = None) -> Any:
        resp = self._client.post(path, json=json, params={k: v for k, v in (params or {}).items() if v is not None})
        self._raise_for_status(resp)
        if resp.status_code == 204:
            return None
        return resp.json()

    def put(self, path: str, json: Any = None) -> Any:
        resp = self._client.put(path, json=json)
        self._raise_for_status(resp)
        if resp.status_code == 204:
            return None
        return resp.json()

    def delete(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        resp = self._client.delete(path, params={k: v for k, v in (params or {}).items() if v is not None})
        self._raise_for_status(resp)
        if resp.status_code == 204:
            return None
        return resp.json()

    def put_binary(self, path: str, data: bytes, content_type: str, params: Dict[str, Any]) -> None:
        resp = self._client.put(
            path,
            content=data,
            headers={"Content-Type": content_type},
            params={k: v for k, v in params.items() if v is not None},
        )
        self._raise_for_status(resp)

    def get_binary(self, path: str, params: Optional[Dict[str, Any]] = None) -> tuple[bytes, str]:
        resp = self._client.get(path, params={k: v for k, v in (params or {}).items() if v is not None})
        self._raise_for_status(resp)
        kind = resp.headers.get("X-Tyto-FS-Kind", "file")
        return resp.content, kind

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
