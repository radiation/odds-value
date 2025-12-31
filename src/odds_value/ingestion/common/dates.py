from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any


def parse_api_sports_game_datetime(value: Any, *, provider_game_id: str) -> datetime:
    """
    Parse api-sports 'game.date' field into tz-aware UTC datetime.

    Supports:
      - ISO string: "2025-09-07T20:20:00Z" / "+00:00"
      - Dict:
        {"timezone":"UTC","date":"YYYY-MM-DD","time":"HH:MM","timestamp": 123}
    """
    if isinstance(value, dict):
        ts = value.get("timestamp")
        if isinstance(ts, int):
            return datetime.fromtimestamp(ts, tz=UTC)

        date_part = value.get("date")
        time_part = value.get("time") or "00:00"
        if not isinstance(date_part, str) or not isinstance(time_part, str):
            raise ValueError(
                f"Missing/invalid game.date dict for provider_game_id={provider_game_id}: {value!r}"
            )

        # Treat as UTC; timestamp is preferred when present.
        return datetime.fromisoformat(f"{date_part}T{time_part}:00+00:00")

    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    raise ValueError(
        f"Missing/invalid game.date for provider_game_id={provider_game_id}: {value!r}"
    )


def parse_odds_api_datetime(value: Any) -> datetime | None:
    """
    Best-effort parser for Odds API timestamps.

    Uses parse_api_sports_game_datetime when possible,
    but returns None instead of raising on bad input.
    """
    if value in (None, ""):
        return None

    try:
        # Odds API uses ISO strings; pass a dummy provider id
        return parse_api_sports_game_datetime(
            value,
            provider_game_id="odds-api",
        )
    except Exception:
        return None


def nfl_week1_bucket_start_utc(season_year: int) -> datetime:
    """Compute the UTC datetime for the start of the NFL Week 1 bucket (Tue 00:00 UTC after Labor Day)."""
    ld = date(season_year, 9, 1)
    while ld.weekday() != 0:  # Monday
        ld += timedelta(days=1)
    tue = ld + timedelta(days=1)
    return datetime(tue.year, tue.month, tue.day, tzinfo=UTC)


def in_nfl_regular_season_window(dt: datetime, season_year: int) -> bool:
    """
    Date-window filter to exclude preseason even when stage/week is missing.
    (Covers regular season + playoffs)
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    season_weeks = 18 if season_year >= 2021 else 17
    start_window = nfl_week1_bucket_start_utc(season_year)
    end_window = start_window + timedelta(weeks=season_weeks)
    return start_window <= dt <= end_window


def compute_week_from_start_time_nfl(dt: datetime, season_year: int) -> int | None:
    """
    Compute NFL week number using Tue→Mon buckets from Week 1 anchor.
    Returns None if dt is outside regular season window.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

    if not in_nfl_regular_season_window(dt, season_year):
        return None

    week1_start = nfl_week1_bucket_start_utc(season_year)
    delta_days = (dt - week1_start).days
    if delta_days < 0:
        return None
    return 1 + (delta_days // 7)
