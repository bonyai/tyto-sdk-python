from __future__ import annotations

import pytest

from tyto._client import TemplateStack
from tyto._proto.tyto.runtime.v1 import tapi_pb2

from test_contract import FakeGuest, FakeTapi, FakeTransport, make_client


def _client(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    return make_client(monkeypatch, transport), transport


def test_list_templates_maps_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    transport.tapi.templates = [
        tapi_pb2.TApiTemplate(
            template_id="bonya-dev",
            version="2",
            digest="sha256:aaa",
            is_default=True,
            metadata=tapi_pb2.TApiTemplateMetadata(
                description="Dev image",
                os="ubuntu",
                os_version="24.04",
                stacks=[tapi_pb2.TApiTemplateStack(name="go", version="1.25")],
                agent_cli_support=["codex"],
            ),
        ),
        tapi_pb2.TApiTemplate(template_id="bonya-dev", version="1", digest="sha256:bbb", is_default=False),
    ]

    templates = client.list_templates()

    assert len(templates) == 2
    first = templates[0]
    assert first.id == "bonya-dev"
    assert first.version == "2"
    assert first.digest == "sha256:aaa"
    assert first.is_default is True
    assert first.metadata.description == "Dev image"
    assert first.metadata.os == "ubuntu"
    assert first.metadata.stacks == (TemplateStack(name="go", version="1.25"),)
    assert first.metadata.agent_cli_support == ("codex",)

    second = templates[1]
    assert second.version == "1"
    assert second.is_default is False


def test_list_templates_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    transport.tapi.templates = []

    templates = client.list_templates()

    assert templates == []
