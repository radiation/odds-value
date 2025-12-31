from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from odds_value.db.enums import SportEnum
from odds_value.ingestion.common.dates import parse_odds_api_datetime
from odds_value.ingestion.odds_api.odds_api_client import OddsApiClient
from odds_value.ingestion.odds_api.odds_api_mappers import map_event_to_snapshots
from odds_value.ingestion.odds_api.odds_api_upsert import (
    find_game_for_odds_event,
    maybe_store_payload,
    upsert_book,
    upsert_odds_snapshot,
)


def ingest_odds(
    session: Session,
    *,
    client: OddsApiClient,
    sport: SportEnum,
    sport_key: str = "americanfootball_nfl",
    regions: str = "us",
    markets: str = "h2h,spreads,totals",
    days_ahead: int = 7,
    store_payloads: bool = True,
    snapshot_at: datetime | None = None,
) -> int:
    params = {
        "regions": regions,
        "markets": markets,
        "oddsFormat": "american",
        "dateFormat": "iso",
    }

    if snapshot_at is None:
        events = client.get_odds(sport_key=sport_key, params=params)
        fetched_at = datetime.now(UTC)
        payload_for_storage: dict[str, Any] | None = None
    else:
        wrapper = client.get_historical_odds(
            sport_key=sport_key, snapshot_at=snapshot_at, params=params
        )
        fetched_at = parse_odds_api_datetime(wrapper.get("timestamp", "")) or snapshot_at
        data = wrapper.get("data", [])
        events = [e for e in data if isinstance(e, dict)]
        payload_for_storage = wrapper  # (optional) store wrapper too

    inserted = 0

    for event in events:
        game = find_game_for_odds_event(
            session,
            sport=sport,
            home_team_name=event.get("home_team", ""),
            away_team_name=event.get("away_team", ""),
            commence_time_iso=event.get("commence_time", ""),
        )
        if game is None:
            continue

        rows = map_event_to_snapshots(event, fetched_at=fetched_at)

        for r in rows:
            book = upsert_book(session, key=r["book_key"], name=r["book_name"])
            if upsert_odds_snapshot(
                session,
                game_id=game.id,
                book_id=book.id,
                captured_at=r["captured_at"],
                market_type=r["market_type"],
                side_type=r["side_type"],
                line=r["line"],
                price=r["price"],
                is_closing=r["is_closing"],
                provider=r["provider"],
            ):
                inserted += 1

        maybe_store_payload(
            session,
            enabled=store_payloads,
            provider="odds-api",
            entity_type="odds_event" if snapshot_at is None else "odds_event_historical",
            entity_key=f"{event.get('id')}_{event.get('commence_time')}",
            fetched_at=fetched_at,
            payload=event,
        )

    # optional: store wrapper once per call
    if payload_for_storage is not None:
        maybe_store_payload(
            session,
            enabled=store_payloads,
            provider="odds-api",
            entity_type="historical_snapshot_wrapper",
            entity_key=str(payload_for_storage.get("timestamp")),
            fetched_at=fetched_at,
            payload=payload_for_storage,
        )

    return inserted
