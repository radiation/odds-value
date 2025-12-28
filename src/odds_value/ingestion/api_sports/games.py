from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import typer
from sqlalchemy import select
from sqlalchemy.orm import Session

from odds_value.db.enums import SeasonTypeEnum, SportEnum
from odds_value.db.models import Game, League, Team
from odds_value.ingestion.api_sports.api_sports_client import ApiSportsClient
from odds_value.ingestion.api_sports.api_sports_upsert import (
    maybe_store_payload,
    upsert_game_from_api_sports_item,
    upsert_league,
    upsert_season,
    upsert_team_game_stats,
)
from odds_value.ingestion.common.dates import (
    in_nfl_regular_season_window,
    parse_api_sports_game_datetime,
)


def is_regular_season_game(item: dict[str, Any]) -> bool:
    game = item.get("game") or {}
    stage = game.get("stage")
    if not isinstance(stage, str):
        return True
    return stage.strip().lower() in {"regular season", "reg", "regular"}


def ingest_games(
    session: Session,
    *,
    client: ApiSportsClient,
    provider_league_id: str,
    league_name: str,
    sport: SportEnum,
    season_year: int,
    season_type: SeasonTypeEnum | None = None,
    store_payloads: bool,
) -> int:
    fetched_at = datetime.now(UTC)

    # Fetch games list
    items = client.get_response_items(
        "/games",
        params={"league": provider_league_id, "season": season_year},
    )
    maybe_store_payload(
        session,
        enabled=store_payloads,
        provider="api-sports",
        entity_type="games",
        entity_key=f"league={provider_league_id};season={season_year}",
        fetched_at=fetched_at,
        payload={"response": items},
    )

    league = upsert_league(
        session,
        provider_league_id=str(provider_league_id),
        name=league_name,
        sport=sport,
    )
    season = upsert_season(
        session,
        league_id=league.id,
        year=season_year,
        season_type=season_type,
        name=str(season_year),
        is_active=False,
    )

    count = 0
    skipped = 0

    for item in items:
        game_obj = item.get("game") or {}
        provider_game_id = str(game_obj.get("id"))

        # Parse start_time *before* upsert so we can filter without side effects
        start_time = parse_api_sports_game_datetime(
            game_obj.get("date"),
            provider_game_id=provider_game_id,
        )

        # Skip preseason reliably even when stage is missing
        if sport == SportEnum.NFL and not in_nfl_regular_season_window(start_time, season_year):
            skipped += 1
            continue

        # Optional: keep your stage-based filter too (harmless)
        if not is_regular_season_game(item):
            skipped += 1
            continue

        upsert_game_from_api_sports_item(
            session,
            league=league,
            season=season,
            item=item,
            source_last_seen_at=fetched_at,
        )
        count += 1

    typer.echo(f"Skipped {skipped} games outside regular-season window")

    return count


def ingest_game_stats(
    session: Session,
    *,
    client: ApiSportsClient,
    provider_game_id: str,
    store_payloads: bool,
) -> int:
    fetched_at = datetime.now(UTC)

    game = session.scalar(select(Game).where(Game.provider_game_id == str(provider_game_id)))
    if not game:
        raise ValueError(
            f"Game not found for provider_game_id={provider_game_id}. Ingest games first."
        )

    # Fetch team statistics for this game
    items = client.get_response_items("/games/statistics", params={"game": provider_game_id})

    maybe_store_payload(
        session,
        enabled=store_payloads,
        provider="api-sports",
        entity_type="game_statistics",
        entity_key=str(provider_game_id),
        fetched_at=fetched_at,
        payload={"response": items},
    )

    # Items are team-scoped stats objects
    # We need internal team rows for provider team ids
    updated = 0
    for row in items:
        team_obj: dict[str, Any] = row.get("team") or {}
        stats_list: list[dict[str, Any]] | None = row.get("statistics")

        provider_team_id = str(team_obj.get("id"))
        team = session.scalar(
            select(Team).where(
                Team.league_id == game.league_id, Team.provider_team_id == provider_team_id
            )
        )
        if not team:
            continue

        is_home = team.id == game.home_team_id
        upsert_team_game_stats(session, game=game, team=team, is_home=is_home, stats=stats_list)
        updated += 1

    return updated


def ingest_games_with_stats(
    session: Session,
    *,
    client: ApiSportsClient,
    provider_league_id: str,
    league_name: str,
    sport: SportEnum,
    season_year: int,
    season_type: SeasonTypeEnum | None,
    store_payloads: bool,
) -> tuple[int, int]:
    games_count = ingest_games(
        session,
        client=client,
        provider_league_id=provider_league_id,
        league_name=league_name,
        sport=sport,
        season_year=season_year,
        season_type=season_type,
        store_payloads=store_payloads,
    )

    # Now pull stats for every game we just ingested for that league/season
    games = session.scalars(
        select(Game).where(
            Game.league_id
            == session.scalar(
                select(League.id).where(League.provider_league_id == provider_league_id)
            )
        )
    ).all()

    stats_rows = 0
    for g in games:
        stats_rows += ingest_game_stats(
            session,
            client=client,
            provider_game_id=g.provider_game_id,
            store_payloads=store_payloads,
        )

    return games_count, stats_rows
