from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from odds_value.db.enums import SportEnum


class ApiSportsAdapter(Protocol):
    sport: SportEnum

    def is_in_scope_game(
        self, item: dict[str, Any], *, season_year: int, start_time_utc: datetime
    ) -> bool: ...
    def compute_week(
        self, item: dict[str, Any], *, start_time_utc: datetime, season_year: int
    ) -> int | None: ...
    def supports_team_stats(self) -> bool: ...
    def stats_endpoint(self) -> str: ...
