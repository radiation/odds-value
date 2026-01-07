from __future__ import annotations

from typing import Any

from odds_value.db.enums import GameStatusEnum


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
