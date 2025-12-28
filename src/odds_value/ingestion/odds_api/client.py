from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True)
class OddsApiClient:
    base_url: str
    api_key: str
    timeout_s: float = 30.0

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        merged = dict(params or {})
        merged["apiKey"] = self.api_key

        with httpx.Client(timeout=self.timeout_s) as client:
            resp = client.get(url, params=merged)
            resp.raise_for_status()
            data: Any = resp.json()

        if not isinstance(data, (dict, list)):
            raise TypeError(f"Unexpected Odds API response type: {type(data)}")
        return data  # Odds API endpoints return lists for /odds, dicts for some others
