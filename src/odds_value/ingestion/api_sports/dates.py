from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
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
            return datetime.fromtimestamp(ts, tz=timezone.utc)

        date_part = value.get("date")
        time_part = value.get("time") or "00:00"
        if not isinstance(date_part, str) or not isinstance(time_part, str):
            raise ValueError(f"Missing/invalid game.date dict for provider_game_id={provider_game_id}: {value!r}")

        # Treat as UTC; timestamp is preferred when present.
        return datetime.fromisoformat(f"{date_part}T{time_part}:00+00:00")

    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    raise ValueError(f"Missing/invalid game.date for provider_game_id={provider_game_id}: {value!r}")


def labor_day(year: int) -> date:
    """First Monday of September."""
    d = date(year, 9, 1)
    while d.weekday() != 0:  # Monday
        d += timedelta(days=1)
    return d


def first_thursday_after_labor_day(year: int) -> date:
    """Thursday after Labor Day."""
    d = labor_day(year) + timedelta(days=1)
    while d.weekday() != 3:  # Thursday
        d += timedelta(days=1)
    return d


def nfl_week1_bucket_start_utc(year: int) -> datetime:
    """
    Stable NFL week bucketing boundary:
    Week 1 kickoff anchor = first Thursday after Labor Day.
    Bucket start = Tuesday 00:00 UTC of that week.
    """
    kickoff = first_thursday_after_labor_day(year)
    kickoff_dt = datetime(kickoff.year, kickoff.month, kickoff.day, tzinfo=timezone.utc)
    return kickoff_dt - timedelta(days=2)  # Tuesday 00:00 UTC


def in_nfl_regular_season_window(dt: datetime, season_year: int) -> bool:
    """
    Date-window filter to exclude preseason even when stage/week is missing.
    (Covers regular season + playoffs)
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    start_window = datetime(season_year, 9, 1, tzinfo=timezone.utc)
    end_window = datetime(season_year + 1, 2, 15, tzinfo=timezone.utc)
    return start_window <= dt <= end_window


def compute_week_from_start_time_nfl(dt: datetime, season_year: int) -> int | None:
    """
    Compute NFL week number using Tue→Mon buckets from Week 1 anchor.
    Returns None if dt is outside regular season window.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    if not in_nfl_regular_season_window(dt, season_year):
        return None

    week1_start = nfl_week1_bucket_start_utc(season_year)
    delta_days = (dt - week1_start).days
    if delta_days < 0:
        return None
    return 1 + (delta_days // 7)
