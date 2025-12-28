from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from odds_value.db.enums import SportEnum
from odds_value.db.models import Book, Game, IngestedPayload, League, OddsSnapshot


# ---------------------------------------------------------
# Small helpers
# ---------------------------------------------------------


def _norm_team_name(s: str) -> str:
    # Keep it intentionally simple (matches api_sports “simple helpers” vibe).
    return " ".join(
        s.lower()
        .replace(".", "")
        .replace("-", " ")
        .replace("&", "and")
        .split()
    )


def _parse_iso_utc(s: str) -> Optional[datetime]:
    """
    Odds API uses ISO strings, often ending with 'Z'.
    Return tz-aware UTC datetime, or None.
    """
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


# ---------------------------------------------------------
# Upserts
# ---------------------------------------------------------


def upsert_book(session: Session, *, key: str, name: str) -> Book:
    book = session.scalar(select(Book).where(Book.key == key))
    if book:
        # keep name fresh if provider changes capitalization
        if book.name != name:
            book.name = name
        if not book.is_active:
            book.is_active = True
        return book

    book = Book(key=key, name=name, is_active=True)
    session.add(book)
    session.flush()
    return book


def find_game_for_odds_event(
    session: Session,
    *,
    sport: SportEnum,
    home_team_name: str,
    away_team_name: str,
    commence_time_iso: str,
    kickoff_tolerance_minutes: int = 15,
) -> Optional[Game]:
    """
    Resolve an Odds API event to a canonical Game.

    MVP strategy:
    - Parse commence_time
    - Query candidate games in a +/- tolerance window for the given sport
    - Match on normalized home/away team names using the Game relationships
    """
    kickoff = _parse_iso_utc(commence_time_iso)
    if kickoff is None:
        return None

    kickoff_for_query = kickoff
    if kickoff_for_query.tzinfo is not None:
        kickoff_for_query = kickoff_for_query.replace(tzinfo=None)

    start_min = kickoff_for_query - timedelta(minutes=kickoff_tolerance_minutes)
    start_max = kickoff_for_query + timedelta(minutes=kickoff_tolerance_minutes)

    home_norm = _norm_team_name(home_team_name)
    away_norm = _norm_team_name(away_team_name)

    # Find candidate games in time window for this sport
    stmt = (
        select(Game)
        .join(League, Game.league_id == League.id)
        .where(League.sport == sport.value)
        .where(Game.start_time >= start_min)
        .where(Game.start_time <= start_max)
    )
    candidates = session.execute(stmt).scalars().all()
    if not candidates:
        return None

    matches: list[Game] = []
    for g in candidates:
        # Use relationship-loaded teams
        if _norm_team_name(g.home_team.name) == home_norm and _norm_team_name(g.away_team.name) == away_norm:
            matches.append(g)

    if len(matches) != 1:
        return None

    return matches[0]


def upsert_odds_snapshot(
    session: Session,
    *,
    game_id: int,
    book_id: int,
    captured_at: datetime,
    market_type: Any,
    side_type: Any,
    line: float | None,
    price: int,
    is_closing: bool,
    provider: str,
) -> bool:
    """
    Insert a single OddsSnapshot if it doesn't already exist.

    Returns True if inserted, False if it already existed.
    """
    stmt = (
        select(OddsSnapshot)
        .where(OddsSnapshot.game_id == game_id)
        .where(OddsSnapshot.book_id == book_id)
        .where(OddsSnapshot.captured_at == captured_at)
        .where(OddsSnapshot.market_type == market_type)
        .where(OddsSnapshot.side_type == side_type)
    )
    if line is None:
        stmt = stmt.where(OddsSnapshot.line.is_(None))
    else:
        stmt = stmt.where(OddsSnapshot.line == line)

    existing = session.scalar(stmt)
    if existing:
        return False

    snap = OddsSnapshot(
        game_id=game_id,
        book_id=book_id,
        captured_at=captured_at,
        market_type=market_type,
        side_type=side_type,
        line=line,
        price=price,
        is_closing=is_closing,
        provider=provider,
    )
    session.add(snap)
    return True


# ---------------------------------------------------------
# Payload storage
# ---------------------------------------------------------


def maybe_store_payload(
    session: Session,
    *,
    enabled: bool,
    provider: str,
    entity_type: str,
    entity_key: str,
    fetched_at: datetime,
    payload: dict[str, Any],
) -> None:
    if not enabled:
        return

    session.add(
        IngestedPayload(
            provider=provider,
            entity_type=entity_type,
            entity_key=entity_key,
            fetched_at=fetched_at,
            payload_json=payload,
        )
    )
