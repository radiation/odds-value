from __future__ import annotations

from datetime import datetime
from typing import Any

from odds_value.db.enums import SportEnum
from odds_value.ingestion.api_sports.adapters.base import ApiSportsAdapter


class MlbAdapter(ApiSportsAdapter):
    sport = SportEnum.MLB

    def is_in_scope_game(
        self, item: dict[str, Any], *, season_year: int, start_time_utc: datetime
    ) -> bool:
        # TODO: Implement pre/post season filtering
        return True

    def compute_week(
        self,
        item: dict[str, Any],
        *,
        start_time_utc: datetime,
        season_year: int,
    ) -> int | None:
        # This is NFL-specific; refactor later
        return None

    def supports_team_stats(self) -> bool:
        # TODO: Add team stats ingestion later
        return False

    def stats_endpoint(self) -> str:
        # Not used while supports_team_stats=False, but required by Protocol
        return "/games/statistics"
