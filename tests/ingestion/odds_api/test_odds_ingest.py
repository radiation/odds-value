from __future__ import annotations

from datetime import UTC, datetime

from odds_value.db.enums import MarketTypeEnum, SideTypeEnum
from odds_value.db.models import League, Season
from odds_value.ingestion.api_sports.api_sports_upsert import upsert_game_from_api_sports_item
from odds_value.ingestion.odds_api.odds_api_upsert import upsert_book, upsert_odds_snapshot


def test_odds_snapshot_upsert_idempotent(
    db_session, league: League, season: Season, now_utc: datetime
) -> None:
    item = {
        "game": {
            "id": "api-sports-game-1",
            "date": "2025-12-28T18:00:00Z",
        },
        "teams": {
            "home": {"name": "Team A"},
            "away": {"name": "Team B"},
        },
    }

    game = upsert_game_from_api_sports_item(
        db_session,
        league=league,
        season=season,
        item=item,
        source_last_seen_at=now_utc,
    )

    book = upsert_book(db_session, key="testbook", name="Test Book")

    captured_at = datetime(2025, 12, 28, 12, 0, tzinfo=UTC)

    first = upsert_odds_snapshot(
        db_session,
        game_id=game.id,
        book_id=book.id,
        captured_at=captured_at,
        market_type=MarketTypeEnum.SPREAD,
        side_type=SideTypeEnum.HOME,
        line=-3.5,
        price=-110,
        is_closing=False,
        provider="odds-api",
    )
    second = upsert_odds_snapshot(
        db_session,
        game_id=game.id,
        book_id=book.id,
        captured_at=captured_at,
        market_type=MarketTypeEnum.SPREAD,
        side_type=SideTypeEnum.HOME,
        line=-3.5,
        price=-110,
        is_closing=False,
        provider="odds-api",
    )

    assert first is True
    assert second is False
