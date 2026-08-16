from __future__ import annotations

import builtins
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Protocol, cast
from urllib.parse import urlsplit, urlunsplit

import grpc

from ._errors import ConnectionError, InvalidRequestError, TimeoutError


# The endpoint used when neither the ``endpoint`` argument nor BONYA_ENDPOINT
# names one. Self-hosted deployments must set one of those explicitly.
DEFAULT_ENDPOINT = "https://api.tyto.run"


class ClosableChannel(Protocol):
    def close(self) -> object: ...


@dataclass(frozen=True)
class NormalizedEndpoint:
    url: str
    target: str


def normalize_endpoint(endpoint: str) -> NormalizedEndpoint:
    raw = endpoint.strip()
    if not raw:
        raise InvalidRequestError("endpoint is required")
    try:
        parts = urlsplit(raw)
    except ValueError as exc:
        raise InvalidRequestError("endpoint is invalid") from exc
    if parts.scheme != "https":
        raise InvalidRequestError("endpoint must use https")
    if parts.username or parts.password:
        raise InvalidRequestError("endpoint must not include credentials")
    if parts.query or parts.fragment:
        raise InvalidRequestError("endpoint must not include query strings or fragments")
    if not parts.hostname:
        raise InvalidRequestError("endpoint requires a host")
    try:
        port = parts.port
    except ValueError as exc:
        raise InvalidRequestError("endpoint has a malformed port") from exc

    host = parts.hostname
    if ":" in host and not host.startswith("["):
        authority = f"[{host}]"
    else:
        authority = host
    if port is not None:
        authority = f"{authority}:{port}"
    path = parts.path.rstrip("/")
    if path == "/":
        path = ""
    url = urlunsplit(("https", authority, path, "", ""))
    target = authority + path
    return NormalizedEndpoint(url=url, target=target)


def channel_credentials(ca_bundle: str | None) -> grpc.ChannelCredentials:
    root_certificates = None
    if ca_bundle:
        try:
            root_certificates = Path(ca_bundle).read_bytes()
        except OSError as exc:
            raise InvalidRequestError("ca_bundle could not be read") from exc
    return grpc.ssl_channel_credentials(root_certificates=root_certificates)


ChannelFactory = Callable[[NormalizedEndpoint, grpc.ChannelCredentials], ClosableChannel]


class ChannelPool:
    def __init__(self, credentials: grpc.ChannelCredentials, factory: ChannelFactory | None = None) -> None:
        self._credentials = credentials
        self._factory = factory or self._new_channel
        self._lock = Lock()
        self._closed = False
        self._channels: dict[str, ClosableChannel] = {}

    def get(self, endpoint: NormalizedEndpoint) -> ClosableChannel:
        with self._lock:
            if self._closed:
                raise InvalidRequestError("Bonya client is closed")
            channel = self._channels.get(endpoint.url)
            if channel is None:
                channel = self._factory(endpoint, self._credentials)
                self._channels[endpoint.url] = channel
            return channel

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            channels = list(self._channels.values())
            self._channels.clear()
        for channel in channels:
            channel.close()

    @staticmethod
    def _new_channel(endpoint: NormalizedEndpoint, credentials: grpc.ChannelCredentials) -> ClosableChannel:
        return cast(ClosableChannel, grpc.secure_channel(endpoint.target, credentials))


@dataclass(frozen=True)
class Deadline:
    expires_at: float

    @classmethod
    def start(cls, timeout: float | None) -> "Deadline":
        if timeout is None:
            return cls(float("inf"))
        if timeout <= 0:
            raise TimeoutError("operation deadline exhausted")
        return cls(time.monotonic() + timeout)

    def remaining(self) -> float:
        remaining = self.expires_at - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("operation deadline exhausted")
        return remaining


def redact(value: str) -> str:
    if not value:
        return value
    return "[redacted]"


def sanitize_message(message: object, secrets: list[str]) -> str:
    text = str(message)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[redacted]")
    text = re.sub(r"(?<!\S)/(?:[A-Za-z0-9._-]+/)*[A-Za-z0-9._-]+", "[redacted-path]", text)
    return text


def sleep_with_deadline(seconds: float, deadline: Deadline) -> None:
    time.sleep(min(seconds, max(0.0, deadline.expires_at - time.monotonic())))
