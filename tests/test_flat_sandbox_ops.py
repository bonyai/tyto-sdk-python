from __future__ import annotations

import pytest

from tyto import Preview, PreviewAuth, SessionInfo, Snapshot

from test_contract import FakeGuest, FakeTapi, FakeTransport, make_client
from test_sessions import FakeSessionGuest, make_sessions_client


def _client(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    return client, transport


def test_create_snapshot_resolves_the_handle_then_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    snapshot = client.create_snapshot(sandbox.id, idempotency_key="snap-key")

    assert isinstance(snapshot, Snapshot)
    assert snapshot.source_sandbox_id == sandbox.id
    assert len(transport.tapi.get_requests) == 1
    assert transport.tapi.snapshot_create_requests[-1].idempotency_key == "snap-key"


def test_delete_snapshot_resolves_the_handle_then_deletes(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    snapshot = client.create_snapshot(sandbox.id)

    client.delete_snapshot(sandbox.id, snapshot.id)

    assert transport.tapi.snapshot_delete_requests[-1].snapshot_id == snapshot.id
    assert transport.tapi.snapshot_delete_requests[-1].source_sandbox_id == sandbox.id


def test_create_snapshot_propagates_get_sandbox_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)

    with pytest.raises(Exception):
        client.create_snapshot("sbx-missing")
    assert len(transport.tapi.snapshot_create_requests) == 0


def test_create_list_delete_preview_resolve_the_handle_then_delegate(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    preview = client.create_preview(sandbox.id, 3000, name="web")
    assert isinstance(preview, Preview)
    assert preview.sandbox_id == sandbox.id
    assert preview.auth is PreviewAuth.TOKEN

    previews = client.list_previews(sandbox.id)
    assert any(p.id == preview.id for p in previews)

    client.delete_preview(sandbox.id, preview.id)
    assert transport.tapi.preview_delete_requests[-1].preview_id == preview.id
    assert transport.tapi.preview_delete_requests[-1].sandbox_id == sandbox.id


def test_create_list_kill_session_resolve_the_handle_then_delegate(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    client, transport = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    created = client.create_session(sandbox.id, "server", ["bash"], cols=120, rows=40)
    assert isinstance(created, SessionInfo)
    assert created.name == "server"
    assert len(transport.tapi.get_requests) == 1
    assert guest.create_requests[-1].cols == 120

    listed = client.list_sessions(sandbox.id)
    assert listed is not None

    killed = client.kill_session(sandbox.id, "server", signal="KILL", grace_ms=1000)
    assert killed.status.value == "killed"
    assert guest.kill_requests[-1].signal == "KILL"
