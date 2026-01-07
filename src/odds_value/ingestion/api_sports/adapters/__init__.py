from __future__ import annotations

from odds_value.db.enums import SportEnum
from odds_value.ingestion.api_sports.adapters.base import ApiSportsAdapter
from odds_value.ingestion.api_sports.adapters.mlb import MlbAdapter
from odds_value.ingestion.api_sports.adapters.nfl import NflAdapter

_ADAPTERS: dict[SportEnum, ApiSportsAdapter] = {
    SportEnum.NFL: NflAdapter(),
    SportEnum.MLB: MlbAdapter(),
}


def get_adapter(sport: SportEnum) -> ApiSportsAdapter:
    try:
        return _ADAPTERS[sport]
    except KeyError as e:
        raise ValueError(f"No api-sports adapter registered for sport={sport}") from e
