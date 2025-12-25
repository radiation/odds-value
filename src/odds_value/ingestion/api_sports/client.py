from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True)
class ApiSportsClient:
    base_url: str
    api_key: str
    timeout_s: float = 30.0

    def _headers(self) -> dict[str, str]:
        return {"x-apisports-key": self.api_key}

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        with httpx.Client(timeout=self.timeout_s, headers=self._headers()) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()

        errors = data.get("errors") or []
        if errors:
            raise RuntimeError(f"api-sports returned errors: {errors}")

        return data

    def get_response_items(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        payload = self.get(path, params=params)
        items = payload.get("response")
        if not isinstance(items, list):
            raise TypeError(f"Expected 'response' list, got: {type(items)}")
        return [i for i in items if isinstance(i, dict)]
