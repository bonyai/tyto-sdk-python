from __future__ import annotations

import os
from dataclasses import dataclass

from .errors import TytoError


@dataclass
class TytoConfig:
    api_key: str = ""
    api_url: str = "https://api.tyto.run"


def resolve_config(
    api_key: str | None = None,
    api_url: str | None = None,
) -> TytoConfig:
    key = api_key or os.environ.get("TYTO_API_KEY", "")
    if not key:
        raise TytoError(
            "api_key is required. Pass it as an argument or set the TYTO_API_KEY environment variable."
        )
    url = (api_url or os.environ.get("TYTO_API_URL", "https://api.tyto.run")).rstrip("/")
    return TytoConfig(api_key=key, api_url=url)
