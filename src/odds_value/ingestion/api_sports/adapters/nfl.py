from __future__ import annotations

from datetime import datetime
from typing import Any

from odds_value.db.enums import SportEnum
from odds_value.ingestion.api_sports.adapters.base import ApiSportsAdapter
from odds_value.ingestion.common.dates import (
    compute_week_from_start_time_nfl,
    in_nfl_regular_season_window,
    parse_nfl_week,
)


class NflAdapter(ApiSportsAdapter):
    sport = SportEnum.NFL

    def is_in_scope_game(
        self, item: dict[str, Any], *, season_year: int, start_time_utc: datetime
    ) -> bool:
        return in_nfl_regular_season_window(dt=start_time_utc, season_year=season_year)

    def compute_week(
        self,
        item: dict[str, Any],
        *,
        start_time_utc: datetime,
        season_year: int,
    ) -> int | None:
        game_obj = item.get("game") or {}
        week_raw = game_obj.get("week")
        week = parse_nfl_week(week_raw)

        if week is not None:
            return week

        # Fallback: compute based on kickoff anchor logic
        return compute_week_from_start_time_nfl(dt=start_time_utc, season_year=season_year)

    def supports_team_stats(self) -> bool:
        return True

    def stats_endpoint(self) -> str:
        return "/games/statistics"
