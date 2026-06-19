from __future__ import annotations

from .config import TytoConfig


def connect_ws(config: TytoConfig, path: str):
    from websockets.sync.client import connect

    ws_url = config.api_url.replace("https://", "wss://").replace("http://", "ws://") + path
    return connect(ws_url, additional_headers={"Authorization": f"Bearer {config.api_key}"})
