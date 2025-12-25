from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from odds_value.db.enums import GameStatusEnum


def parse_api_sports_game_datetime(
    value: Any,
    *,
    provider_game_id: str,
) -> datetime:
    """
    Parse api-sports game date field into a tz-aware UTC datetime.

    Supports:
    - ISO string: "2025-09-07T20:20:00+00:00"
    - Dict format:
      {
        "timezone": "UTC",
        "date": "YYYY-MM-DD",
        "time": "HH:MM",
        "timestamp": 1754006400
      }
    """

    # Case 1: american-football dict format
    if isinstance(value, dict):
        ts = value.get("timestamp")
        if isinstance(ts, int):
            return datetime.fromtimestamp(ts, tz=timezone.utc)

        date_part = value.get("date")
        time_part = value.get("time") or "00:00"

        if not isinstance(date_part, str):
            raise ValueError(
                f"Missing/invalid game.date.date for provider_game_id={provider_game_id}: {value}"
            )

        return datetime.fromisoformat(f"{date_part}T{time_part}:00+00:00")

    # Case 2: ISO string
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    # Anything else → schema drift
    raise ValueError(
        f"Missing/invalid game.date for provider_game_id={provider_game_id}: {value!r}"
    )


def map_game_status(short_code: str | None) -> GameStatusEnum:
    code = (short_code or "").upper()

    # Common api-sports codes
    if code in {"NS", "TBD"}:
        return GameStatusEnum.SCHEDULED
    if code in {"FT", "AET", "FINAL"}:
        return GameStatusEnum.FINAL
    if code in {"PST", "POSTP"}:
        return GameStatusEnum.POSTPONED
    if code in {"CANC", "CANCL", "ABD"}:
        return GameStatusEnum.CANCELED
    if code in {"1Q", "2Q", "3Q", "4Q", "OT", "HT", "LIVE", "IN"}:
        return GameStatusEnum.IN_PROGRESS

    return GameStatusEnum.UNKNOWN


def stats_list_to_map(stats: list[dict[str, Any]] | None) -> dict[str, Any]:
    if not stats:
        return {}

    out: dict[str, Any] = {}
    for item in stats:
        name = item.get("name")
        val = item.get("value")
        if isinstance(name, str) and name:
            out[name] = val
    return out


def coerce_int(val: Any) -> int | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return int(float(s))
        except ValueError:
            return None
    return None
