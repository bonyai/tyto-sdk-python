from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from ._errors import InvalidRequestError
from ._grpc_errors import map_rpc_error
from ._transport import Deadline
from ._proto.tyto.runtime.v1 import preview_pb2, tapi_pb2

if TYPE_CHECKING:
    from ._sandbox import Sandbox

_tapi_pb2: Any = tapi_pb2
_preview_pb2: Any = preview_pb2

_MIN_PREVIEW_PORT = 1024
_MAX_PREVIEW_PORT = 65535
_MAX_PREVIEW_NAME_BYTES = 80

_TOKEN_QUERY_PARAM = "bonya_token"


class PreviewAuth(str, Enum):
    """How a preview URL admits a request."""

    #: The sandbox's data-plane capability admits the request, as a bearer
    #: token or through the browser exchange.
    TOKEN = "token"
    #: No authentication. Anyone holding the URL reaches the service.
    PUBLIC = "public"


_AUTH_TO_PROTO = {
    PreviewAuth.TOKEN: _preview_pb2.PREVIEW_AUTH_MODE_TOKEN,
    PreviewAuth.PUBLIC: _preview_pb2.PREVIEW_AUTH_MODE_PUBLIC,
}
_PROTO_TO_AUTH = {
    _preview_pb2.PREVIEW_AUTH_MODE_TOKEN: PreviewAuth.TOKEN,
    _preview_pb2.PREVIEW_AUTH_MODE_PUBLIC: PreviewAuth.PUBLIC,
}


@dataclass(frozen=True)
class Preview:
    """A published preview URL for one guest port."""

    id: str
    sandbox_id: str
    port: int
    auth: PreviewAuth
    name: str
    url: str
    created_at: datetime


def _preview_from_info(info: Any) -> Preview:
    record = info.record
    created = getattr(record, "created_at_unix_nanos", 0)
    return Preview(
        id=record.preview_id,
        sandbox_id=record.sandbox_id,
        port=record.port,
        # An unrecognised mode is reported as TOKEN rather than guessed open:
        # a client from a future release must never describe a locked preview
        # as public.
        auth=_PROTO_TO_AUTH.get(record.auth_mode, PreviewAuth.TOKEN),
        name=record.name,
        url=info.url,
        created_at=datetime.fromtimestamp(created / 1e9, tz=timezone.utc),
    )


class SandboxPreviews:
    """Preview URL operations for one sandbox.

    These are TApi calls authenticated with the API key, not data-plane calls,
    so the capability-refresh wrapper that guards exec and files does not apply
    here -- there is no capability in play on the request.
    """

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox

    def create(
        self,
        port: int,
        *,
        auth: PreviewAuth = PreviewAuth.TOKEN,
        name: str | None = None,
        idempotency_key: str | None = None,
    ) -> Preview:
        """Publish a preview URL for a guest port.

        On success the sandbox's stored capability is replaced with the one
        returned. The preview scope is newer than the capability a sandbox was
        created with, and a token lacking it is refused by the preview ingress
        with a permission error that is deliberately not a refresh signal, so
        create hands back a usable token rather than leaving the caller to
        discover the gap on their first request.
        """
        if not isinstance(port, int) or isinstance(port, bool):
            raise InvalidRequestError("port must be an integer", sandbox_id=self._sandbox.id)
        if port < _MIN_PREVIEW_PORT or port > _MAX_PREVIEW_PORT:
            raise InvalidRequestError(
                f"port must be between {_MIN_PREVIEW_PORT} and {_MAX_PREVIEW_PORT}",
                sandbox_id=self._sandbox.id,
            )
        if not isinstance(auth, PreviewAuth):
            raise InvalidRequestError("auth must be a PreviewAuth", sandbox_id=self._sandbox.id)
        display_name = name or ""
        if len(display_name.encode("utf-8")) > _MAX_PREVIEW_NAME_BYTES:
            raise InvalidRequestError(
                f"name exceeds {_MAX_PREVIEW_NAME_BYTES} bytes",
                sandbox_id=self._sandbox.id,
            )
        key = idempotency_key or str(uuid.uuid4())
        if not key:
            raise InvalidRequestError("idempotency key must be non-empty", sandbox_id=self._sandbox.id)

        request = _tapi_pb2.TApiCreatePreviewRequest(
            api_key=self._sandbox._client._api_key,
            sandbox_id=self._sandbox.id,
            port=port,
            auth_mode=_AUTH_TO_PROTO[auth],
            name=display_name,
            idempotency_key=key,
        )
        deadline = Deadline.start(self._sandbox._client._timeout)
        try:
            stub: Any = self._sandbox._client._tapi_stub()
            response = stub.CreatePreview(request, timeout=deadline.remaining())
        except Exception as error:  # noqa: BLE001 - re-raised as a typed error
            raise map_rpc_error(
                error,
                secrets=self._sandbox._client._secrets(self._sandbox._capability),
                sandbox_id=self._sandbox.id,
            ) from error

        capability = getattr(response, "capability_jws", "")
        if capability:
            self._sandbox._capability = capability
        if not response.preview.record.preview_id:
            raise InvalidRequestError(
                "CreatePreview response is missing the preview identity",
                sandbox_id=self._sandbox.id,
                idempotency_key=key,
            )
        return _preview_from_info(response.preview)

    def list(self) -> list[Preview]:
        """Every published preview for this sandbox."""
        request = _tapi_pb2.TApiListPreviewsRequest(
            api_key=self._sandbox._client._api_key,
            sandbox_id=self._sandbox.id,
        )
        deadline = Deadline.start(self._sandbox._client._timeout)
        try:
            stub: Any = self._sandbox._client._tapi_stub()
            response = stub.ListPreviews(request, timeout=deadline.remaining())
        except Exception as error:  # noqa: BLE001 - re-raised as a typed error
            raise map_rpc_error(
                error,
                secrets=self._sandbox._client._secrets(self._sandbox._capability),
                sandbox_id=self._sandbox.id,
            ) from error
        return [_preview_from_info(info) for info in response.previews]

    def delete(self, preview_id: str) -> None:
        """Revoke a preview URL."""
        if not preview_id:
            raise InvalidRequestError("preview id is required", sandbox_id=self._sandbox.id)
        request = _tapi_pb2.TApiDeletePreviewRequest(
            api_key=self._sandbox._client._api_key,
            sandbox_id=self._sandbox.id,
            preview_id=preview_id,
        )
        deadline = Deadline.start(self._sandbox._client._timeout)
        try:
            stub: Any = self._sandbox._client._tapi_stub()
            stub.DeletePreview(request, timeout=deadline.remaining())
        except Exception as error:  # noqa: BLE001 - re-raised as a typed error
            raise map_rpc_error(
                error,
                secrets=self._sandbox._client._secrets(self._sandbox._capability),
                sandbox_id=self._sandbox.id,
            ) from error

    def browser_url(self, preview: Preview) -> str:
        """A one-time URL that logs a browser into a token-mode preview.

        The gateway validates the token, trades it for a host-scoped HttpOnly
        cookie, and redirects to the same URL without it -- so no page is ever
        rendered at an address containing the credential. Open it once; the
        cookie carries the session from there.

        This is never a content URL, and it must not be shared: anyone who
        receives it holds the sandbox's data-plane capability until it expires.

        Raises on a public preview, which has no token to exchange and whose
        plain ``url`` already works.
        """
        if preview.auth is PreviewAuth.PUBLIC:
            raise InvalidRequestError(
                "a public preview needs no token; use preview.url",
                sandbox_id=self._sandbox.id,
            )
        capability = self._sandbox._capability
        if not capability:
            raise InvalidRequestError(
                "no capability is available for this sandbox",
                sandbox_id=self._sandbox.id,
            )
        separator = "&" if "?" in preview.url else "?"
        return f"{preview.url}{separator}{_TOKEN_QUERY_PARAM}={capability}"


__all__ = ["Preview", "PreviewAuth", "SandboxPreviews"]
