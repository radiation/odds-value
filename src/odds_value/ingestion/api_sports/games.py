from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import typer
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from odds_value.db.enums import SportEnum
from odds_value.db.models import Game, Team
from odds_value.ingestion.api_sports.adapters import get_adapter
from odds_value.ingestion.api_sports.api_sports_client import ApiSportsClient
from odds_value.ingestion.api_sports.api_sports_upsert import (
    maybe_store_payload,
    upsert_game_from_api_sports_item,
    upsert_league,
    upsert_season,
    upsert_team_game_stats,
)
from odds_value.ingestion.common.dates import parse_api_sports_game_datetime


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
    store_payloads: bool,
) -> list[int]:
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
        name=str(season_year),
        is_active=False,
    )

    skipped = 0

    adapter = get_adapter(sport)

    game_ids: list[int] = []

    for item in items:
        game_obj = item.get("game") or {}
        provider_game_id = str(game_obj.get("id"))

        # Parse start_time *before* upsert so we can filter without side effects
        start_time = parse_api_sports_game_datetime(
            game_obj.get("date"),
            provider_game_id=provider_game_id,
        )

        if not adapter.is_in_scope_game(item, season_year=season_year, start_time_utc=start_time):
            skipped += 1
            continue

        game = upsert_game_from_api_sports_item(
            session,
            league=league,
            season=season,
            item=item,
            source_last_seen_at=fetched_at,
        )
        if game:
            game_ids.append(game.id)
        else:
            typer.echo(f"Warning: failed to upsert game for provider_game_id={provider_game_id}")

        if len(game_ids) % 16 == 0:
            typer.echo(
                f"Ingested {len(game_ids)} games for league={provider_league_id}, season={season_year}"
            )

    typer.echo(f"Skipped {skipped} games outside regular-season window")

    min_dt, max_dt = session.execute(
        select(func.min(Game.start_time), func.max(Game.start_time)).where(
            Game.season_id == season.id
        )
    ).one()

    if min_dt and max_dt:
        season.start_date = min_dt.date()
        season.end_date = max_dt.date()
        session.flush()

    return game_ids


def ingest_game_stats(
    session: Session,
    *,
    client: ApiSportsClient,
    game: Game,
    store_payloads: bool,
) -> int:
    fetched_at = datetime.now(UTC)
    provider_game_id = game.provider_game_id
    if not game:
        raise ValueError(
            f"Game not found for provider_game_id={provider_game_id}. Ingest games first."
        )

    # Fetch team statistics for this game
    items = client.get_response_items(
        "/games/statistics/teams?game=", params={"id": provider_game_id}
    )

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
            typer.echo(
                f"Warning: team not found for provider_team_id={provider_team_id} in game id={provider_game_id}. Skipping stats upsert."
            )
            continue

        is_home = team.id == game.home_team_id
        upsert_team_game_stats(session, game=game, team=team, is_home=is_home, stats=stats_list)
        updated += 1

    typer.echo(f"Upserted team_game_stats for {updated} teams for game id={provider_game_id}")
    return updated


def ingest_games_with_stats(
    session: Session,
    *,
    client: ApiSportsClient,
    provider_league_id: str,
    league_name: str,
    sport: SportEnum,
    season_year: int,
    store_payloads: bool,
) -> tuple[int, int]:
    game_ids = ingest_games(
        session,
        client=client,
        provider_league_id=provider_league_id,
        league_name=league_name,
        sport=sport,
        season_year=season_year,
        store_payloads=store_payloads,
    )

    games = session.scalars(select(Game).where(Game.id.in_(game_ids))).all()
    total_games = len(games)

    typer.echo(f"{total_games} games found in DB for statistics ingestion...")

    stats_rows = 0
    for i, g in enumerate(games, start=1):
        stats_rows += ingest_game_stats(
            session,
            client=client,
            game=g,
            store_payloads=store_payloads,
        )
        typer.echo(f"Processed {i}/{total_games} game statistics rows...")

    return len(game_ids), stats_rows
