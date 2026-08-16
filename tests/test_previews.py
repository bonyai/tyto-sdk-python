from __future__ import annotations

import grpc
import pytest

from tyto import InvalidRequestError, PreviewAuth, Wait
from tyto._proto.tyto.runtime.v1 import preview_pb2

from test_contract import FakeGuest, FakeTapi, FakeTransport, RpcFailure, make_client


def _sandbox(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(
        template="ubuntu-24.04",
        version=None,
        wait=Wait.NONE,
        idempotency_key="idem-1",
    )
    return sandbox, transport


def test_create_returns_the_published_preview(monkeypatch: pytest.MonkeyPatch) -> None:
    sandbox, transport = _sandbox(monkeypatch)

    preview = sandbox.previews.create(3000, name="web")

    assert preview.id == transport.tapi.next_preview_id
    assert preview.sandbox_id == sandbox.id
    assert preview.port == 3000
    assert preview.auth is PreviewAuth.TOKEN
    assert preview.name == "web"
    assert preview.url == f"https://{preview.id}.preview.example.test"
    assert preview.created_at.year == 2023

    sent = transport.tapi.preview_create_requests[-1]
    assert sent.sandbox_id == sandbox.id
    assert sent.auth_mode == preview_pb2.PREVIEW_AUTH_MODE_TOKEN
    # A key is generated per call so a retried create is recognised rather than
    # publishing a second URL.
    assert sent.idempotency_key


def test_create_replaces_the_stored_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    """The preview scope is newer than the token a sandbox was created with.

    Create hands back one that carries it; if the SDK kept the old token, the
    caller's very next preview request would be refused with a permission error
    that is deliberately not a refresh signal.
    """
    sandbox, transport = _sandbox(monkeypatch)
    before = sandbox._capability
    transport.tapi.preview_capability_value = "cap-with-preview-scope"

    sandbox.previews.create(3000)

    assert before != "cap-with-preview-scope"
    assert sandbox._capability == "cap-with-preview-scope"


def test_create_forwards_public_mode_explicitly(monkeypatch: pytest.MonkeyPatch) -> None:
    sandbox, transport = _sandbox(monkeypatch)

    preview = sandbox.previews.create(8080, auth=PreviewAuth.PUBLIC)

    assert preview.auth is PreviewAuth.PUBLIC
    assert transport.tapi.preview_create_requests[-1].auth_mode == preview_pb2.PREVIEW_AUTH_MODE_PUBLIC


@pytest.mark.parametrize(
    "kwargs",
    [
        {"port": 80},
        {"port": 0},
        {"port": 70000},
        {"port": 3000, "name": "x" * 81},
        {"port": 3000, "auth": "token"},
    ],
)
def test_create_validates_before_calling_the_server(
    monkeypatch: pytest.MonkeyPatch, kwargs: dict[str, object]
) -> None:
    sandbox, transport = _sandbox(monkeypatch)

    with pytest.raises(InvalidRequestError):
        sandbox.previews.create(**kwargs)  # type: ignore[arg-type]
    assert transport.tapi.preview_create_requests == []


def test_list_and_delete_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    sandbox, transport = _sandbox(monkeypatch)
    created = sandbox.previews.create(3000, name="web")

    listed = sandbox.previews.list()
    assert [preview.id for preview in listed] == [created.id]
    assert listed[0].url == created.url

    sandbox.previews.delete(created.id)
    assert sandbox.previews.list() == []
    assert transport.tapi.preview_delete_requests[-1].preview_id == created.id


def test_delete_requires_a_preview_id(monkeypatch: pytest.MonkeyPatch) -> None:
    sandbox, transport = _sandbox(monkeypatch)

    with pytest.raises(InvalidRequestError):
        sandbox.previews.delete("")
    assert transport.tapi.preview_delete_requests == []


def test_browser_url_carries_the_current_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    sandbox, transport = _sandbox(monkeypatch)
    transport.tapi.preview_capability_value = "cap-abc"
    preview = sandbox.previews.create(3000)

    url = sandbox.previews.browser_url(preview)

    assert url == f"{preview.url}?bonya_token=cap-abc"


def test_browser_url_refuses_a_public_preview(monkeypatch: pytest.MonkeyPatch) -> None:
    """A public preview has no token to exchange, and attaching one anyway
    would hand the sandbox's capability to whoever the URL is shared with."""
    sandbox, _ = _sandbox(monkeypatch)
    preview = sandbox.previews.create(8080, auth=PreviewAuth.PUBLIC)

    with pytest.raises(InvalidRequestError):
        sandbox.previews.browser_url(preview)


def test_preview_rpc_errors_are_typed(monkeypatch: pytest.MonkeyPatch) -> None:
    sandbox, transport = _sandbox(monkeypatch)
    transport.tapi.preview_create_errors.put(
        RpcFailure(grpc.StatusCode.INVALID_ARGUMENT, "port must be between 1024 and 65535")
    )

    with pytest.raises(InvalidRequestError):
        sandbox.previews.create(3000)


def test_preview_errors_do_not_leak_the_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    """The capability travels in a preview URL, so it must be redacted from
    anything an error message carries."""
    sandbox, transport = _sandbox(monkeypatch)
    secret = sandbox._capability
    transport.tapi.preview_list_errors.put(
        RpcFailure(grpc.StatusCode.INVALID_ARGUMENT, f"bad token {secret}")
    )

    with pytest.raises(InvalidRequestError) as raised:
        sandbox.previews.list()
    assert secret not in str(raised.value)


def test_unknown_auth_mode_is_reported_as_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """A client older than the server must never describe a locked preview as
    public because it did not recognise the enum value."""
    from tyto._previews import _preview_from_info

    record = preview_pb2.PreviewRecord(
        preview_id="pv-aaaaaaaaaaaaaaaaaaaaaaaaaa",
        sandbox_id="sbx-1",
        port=3000,
        auth_mode=99,
    )
    info = type("Info", (), {"record": record, "url": "https://example.test"})()

    assert _preview_from_info(info).auth is PreviewAuth.TOKEN
