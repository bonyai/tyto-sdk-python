from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from ._proto.tyto.runtime.v1 import preview_pb2, tapi_pb2

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


__all__ = ["Preview", "PreviewAuth"]
