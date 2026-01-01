from __future__ import annotations

from typing import Annotated

import typer

from odds_value.cli.common import session_scope
from odds_value.core.config import settings
from odds_value.db.enums import SportEnum
from odds_value.ingestion.api_sports.api_sports_client import ApiSportsClient
from odds_value.ingestion.api_sports.games import (
    ingest_game_stats,
    ingest_games,
    ingest_games_with_stats,
)

api_sports_app = typer.Typer(no_args_is_help=True)


def _api_sports_client() -> ApiSportsClient:
    return ApiSportsClient(
        base_url=settings.api_sports_base_url,
        api_key=settings.require_api_sports_key(),
    )


@api_sports_app.command("games")
def ingest_games_cmd(
    provider_league_id: Annotated[str, typer.Option(..., help="api-sports league id")],
    league_name: Annotated[str, typer.Option(..., help="Human name to store (e.g., NFL)")],
    season_year: Annotated[int, typer.Option(..., help="Season year (e.g., 2024)")],
    sport: Annotated[SportEnum, typer.Option(help="Sport enum")] = SportEnum.NFL,
) -> None:
    with session_scope() as session:
        client = _api_sports_client()
        count = ingest_games(
            session,
            client=client,
            provider_league_id=provider_league_id,
            league_name=league_name,
            sport=sport,
            season_year=season_year,
            store_payloads=settings.store_ingested_payloads,
        )
        session.commit()
    typer.echo(f"Ingested/updated games: {count}")


@api_sports_app.command("game-stats")
def ingest_game_stats_cmd(
    provider_game_id: str = typer.Option(..., help="api-sports game id"),
) -> None:
    with session_scope() as session:
        client = _api_sports_client()
        updated = ingest_game_stats(
            session,
            client=client,
            provider_game_id=provider_game_id,
            store_payloads=settings.store_ingested_payloads,
        )
        session.commit()
    typer.echo(f"Upserted team_game_stats rows: {updated}")


@api_sports_app.command("games-with-stats")
def ingest_games_with_stats_cmd(
    provider_league_id: Annotated[
        str,
        typer.Option(..., help="api-sports league id"),
    ],
    league_name: Annotated[
        str,
        typer.Option(..., help="Human name to store (e.g., NFL)"),
    ],
    season_year: Annotated[
        int,
        typer.Option(..., help="Season year (e.g., 2024)"),
    ],
    sport: Annotated[
        SportEnum,
        typer.Option(help="Sport enum"),
    ] = SportEnum.NFL,
) -> None:
    with session_scope() as session:
        client = _api_sports_client()
        games_count, stats_rows = ingest_games_with_stats(
            session,
            client=client,
            provider_league_id=provider_league_id,
            league_name=league_name,
            sport=sport,
            season_year=season_year,
            store_payloads=settings.store_ingested_payloads,
        )
        session.commit()
    typer.echo(
        f"Ingested/updated games: {games_count}; upserted team_game_stats rows: {stats_rows}"
    )
