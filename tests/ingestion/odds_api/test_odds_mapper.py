from datetime import UTC, datetime

from odds_value.db.enums import MarketTypeEnum, SideTypeEnum
from odds_value.ingestion.odds_api.odds_api_mappers import map_event_to_snapshots


def test_maps_basic_spread_market() -> None:
    fetched_at = datetime(2025, 12, 28, 12, 0, tzinfo=UTC)

    event = {
        "home_team": "Team A",
        "away_team": "Team B",
        "bookmakers": [
            {
                "key": "testbook",
                "title": "Test Book",
                "last_update": "2025-12-28T12:00:00Z",
                "markets": [
                    {
                        "key": "spreads",
                        "last_update": "2025-12-28T12:05:00Z",
                        "outcomes": [
                            {"name": "Team A", "price": -110, "point": -3.5},
                            {"name": "Team B", "price": -110, "point": 3.5},
                        ],
                    }
                ],
            }
        ],
    }

    rows = map_event_to_snapshots(event, fetched_at=fetched_at, provider="odds-api")

    assert len(rows) == 2

    # common invariants
    for r in rows:
        assert r["book_key"] == "testbook"
        assert r["book_name"] == "Test Book"
        assert r["market_type"] == MarketTypeEnum.SPREAD
        assert r["price"] == -110
        assert r["captured_at"].tzinfo == UTC  # normalized by mapper
        assert r["is_closing"] is False
        assert r["provider"] == "odds-api"

    # side/line invariants
    by_side = {r["side_type"]: r for r in rows}
    assert by_side[SideTypeEnum.HOME]["line"] == -3.5
    assert by_side[SideTypeEnum.AWAY]["line"] == 3.5
