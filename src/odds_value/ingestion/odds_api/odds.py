from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy.orm import Session

from odds_value.db.enums import SportEnum
from odds_value.ingestion.odds_api.client import OddsApiClient
from odds_value.ingestion.odds_api.mappers import map_event_to_snapshots
from odds_value.ingestion.odds_api.upsert import (
    upsert_book,
    find_game_for_odds_event,
    upsert_odds_snapshot,
    maybe_store_payload,
)


def ingest_odds(
    session: Session,
    *,
    client: OddsApiClient,
    sport: SportEnum,
    regions: str = "us",
    markets: str = "h2h,spreads,totals",
    days_ahead: int = 7,
    store_payloads: bool = True,
) -> int:
    """
    Ingest main-line odds from Odds API.

    Returns number of odds_snapshots inserted.
    """
    captured_at = datetime.now(timezone.utc)

    payload = client.get(
        f"/sports/americanfootball_nfl/odds",
        params={
            "regions": regions,
            "markets": markets,
            "oddsFormat": "american",
            "dateFormat": "iso",
        },
    )

    inserted = 0

    for event in payload:
        # Resolve canonical game
        game = find_game_for_odds_event(
            session,
            sport=sport,
            home_team_name=event.get("home_team", ""),
            away_team_name=event.get("away_team", ""),
            commence_time_iso=event.get("commence_time", ""),
        )
        if game is None:
            print("Resolver miss:", event["home_team"], "vs", event["away_team"], event["commence_time"])
            continue

        print("Resolved game:", game.id, game.start_time, game.home_team.name, "vs", game.away_team.name)

        # Map event → flat snapshot rows
        fetched_at = datetime.now(timezone.utc)
        rows = map_event_to_snapshots(event, fetched_at=fetched_at)

        for r in rows:
            book = upsert_book(
                session,
                key=r["book_key"],
                name=r["book_name"],
            )

            did_insert = upsert_odds_snapshot(
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
            )

            if did_insert:
                inserted += 1

        # Optional: store raw payload (same pattern as api_sports)
        maybe_store_payload(
            session,
            enabled=store_payloads,
            provider="odds-api",
            entity_type="odds_event",
            entity_key=str(event.get("id") or event.get("commence_time")),
            fetched_at=captured_at,
            payload=event,
        )

    return inserted
