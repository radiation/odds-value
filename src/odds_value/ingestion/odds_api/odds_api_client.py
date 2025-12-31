from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx


def _iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class OddsApiClient:
    base_url: str
    api_key: str
    timeout_s: float = 30.0

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any] | list[Any]:
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        merged = dict(params or {})
        merged["apiKey"] = self.api_key

        with httpx.Client(timeout=self.timeout_s) as client:
            resp = client.get(url, params=merged)
            resp.raise_for_status()
            data: Any = resp.json()

        if not isinstance(data, dict | list):
            raise TypeError(f"Unexpected Odds API response type: {type(data)}")
        return data

    def get_odds(self, *, sport_key: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        data = self.get(f"/sports/{sport_key}/odds", params=params)
        if not isinstance(data, list):
            raise TypeError(f"Expected list for /odds; got {type(data)}")
        return [e for e in data if isinstance(e, dict)]

    def get_historical_odds(
        self, *, sport_key: str, snapshot_at: datetime, params: dict[str, Any]
    ) -> dict[str, Any]:
        # returns the wrapper dict
        merged = dict(params)
        merged["date"] = _iso_z(snapshot_at)
        data = self.get(f"/historical/sports/{sport_key}/odds", params=merged)
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict wrapper for /historical/.../odds; got {type(data)}")
        return data
