from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from odds_value.db.enums import MarketTypeEnum, SideTypeEnum


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------


def _parse_iso_utc(s: str) -> Optional[datetime]:
    try:
        # Odds API uses "...Z"
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        # Normalize to UTC and strip microseconds for stable equality in SQLite
        dt = dt.astimezone(timezone.utc).replace(microsecond=0)
        return dt
    except Exception:
        return None


def _norm(s: str) -> str:
    return " ".join(
        s.lower()
        .replace(".", "")
        .replace("-", " ")
        .replace("&", "and")
        .split()
    )


def _side_home_away(*, outcome_name: str, home_team: str, away_team: str) -> Optional[SideTypeEnum]:
    n = _norm(outcome_name)
    if n == _norm(home_team):
        return SideTypeEnum.HOME
    if n == _norm(away_team):
        return SideTypeEnum.AWAY
    return None


def _side_over_under(*, outcome_name: str) -> Optional[SideTypeEnum]:
    n = _norm(outcome_name)
    if n == "over":
        return SideTypeEnum.OVER
    if n == "under":
        return SideTypeEnum.UNDER
    return None


# ---------------------------------------------------------
# Mapper
# ---------------------------------------------------------


def map_event_to_snapshots(
    event: dict[str, Any],
    *,
    fetched_at: datetime,
    provider: str = "odds-api",
) -> list[dict[str, Any]]:
    """
    Flatten an Odds API event payload into rows for odds_snapshots.

    Rows include:
      - book_key, book_name
      - captured_at (market.last_update > bookmaker.last_update > fetched_at)
      - market_type, side_type, line, price
      - is_closing (False for now)
      - provider
    """
    home_team = event.get("home_team")
    away_team = event.get("away_team")
    if not isinstance(home_team, str) or not isinstance(away_team, str):
        return []

    rows: list[dict[str, Any]] = []

    bookmakers = event.get("bookmakers") or []
    if not isinstance(bookmakers, list):
        return []

    for bm in bookmakers:
        if not isinstance(bm, dict):
            continue

        book_key = bm.get("key")
        book_name = bm.get("title")
        if not isinstance(book_key, str) or not isinstance(book_name, str):
            continue

        bm_last_update = bm.get("last_update")
        bm_captured_at = _parse_iso_utc(bm_last_update) if isinstance(bm_last_update, str) else None

        markets = bm.get("markets") or []
        if not isinstance(markets, list):
            continue

        for m in markets:
            if not isinstance(m, dict):
                continue

            market_key = m.get("key")
            if not isinstance(market_key, str):
                continue

            # Determine captured_at from market last_update first
            m_last_update = m.get("last_update")
            m_captured_at = (
                _parse_iso_utc(m_last_update) if isinstance(m_last_update, str) else None
            )

            # captured_at priority:
            # 1) market.last_update
            # 2) bookmaker.last_update
            # 3) fetched_at (fallback)
            captured_at = m_captured_at or bm_captured_at or fetched_at.replace(microsecond=0)
            if m_captured_at is None and bm_captured_at is None:
                print("WARN: last_update parse failed; falling back to fetched_at:", m.get("last_update"), bm.get("last_update"))

            outcomes = m.get("outcomes") or []
            if not isinstance(outcomes, list):
                continue

            # -------------------------
            # MONEYLINE (h2h)
            # -------------------------
            if market_key == "h2h":
                for o in outcomes:
                    if not isinstance(o, dict):
                        continue
                    name = o.get("name")
                    price = o.get("price")
                    if not isinstance(name, str) or not isinstance(price, int):
                        continue

                    side = _side_home_away(outcome_name=name, home_team=home_team, away_team=away_team)
                    if side is None:
                        continue

                    rows.append(
                        {
                            "book_key": book_key,
                            "book_name": book_name,
                            "captured_at": captured_at,
                            "market_type": MarketTypeEnum.MONEYLINE,
                            "side_type": side,
                            "line": None,
                            "price": price,
                            "is_closing": False,
                            "provider": provider,
                        }
                    )

            # -------------------------
            # SPREADS
            # -------------------------
            elif market_key == "spreads":
                for o in outcomes:
                    if not isinstance(o, dict):
                        continue
                    name = o.get("name")
                    price = o.get("price")
                    point = o.get("point")
                    if not isinstance(name, str) or not isinstance(price, int) or not isinstance(point, (int, float)):
                        continue

                    side = _side_home_away(outcome_name=name, home_team=home_team, away_team=away_team)
                    if side is None:
                        continue

                    rows.append(
                        {
                            "book_key": book_key,
                            "book_name": book_name,
                            "captured_at": captured_at,
                            "market_type": MarketTypeEnum.SPREAD,
                            "side_type": side,
                            "line": float(point),
                            "price": price,
                            "is_closing": False,
                            "provider": provider,
                        }
                    )

            # -------------------------
            # TOTALS
            # -------------------------
            elif market_key == "totals":
                for o in outcomes:
                    if not isinstance(o, dict):
                        continue
                    name = o.get("name")
                    price = o.get("price")
                    point = o.get("point")
                    if not isinstance(name, str) or not isinstance(price, int) or not isinstance(point, (int, float)):
                        continue

                    side = _side_over_under(outcome_name=name)
                    if side is None:
                        continue

                    rows.append(
                        {
                            "book_key": book_key,
                            "book_name": book_name,
                            "captured_at": captured_at,
                            "market_type": MarketTypeEnum.TOTAL,
                            "side_type": side,
                            "line": float(point),
                            "price": price,
                            "is_closing": False,
                            "provider": provider,
                        }
                    )

            # ignore other markets for MVP

    return rows
