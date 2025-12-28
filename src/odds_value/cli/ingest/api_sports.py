from __future__ import annotations

import typer
from sqlalchemy.orm import Session

from odds_value.core.config import settings
from odds_value.db import DatabaseConfig, create_db_engine, create_session_factory
from odds_value.db.enums import SeasonTypeEnum, SportEnum
from odds_value.ingestion.api_sports.client import ApiSportsClient
from odds_value.ingestion.api_sports.games import ingest_game_stats, ingest_games, ingest_games_with_stats

api_sports_app = typer.Typer(no_args_is_help=True)


def _session() -> Session:
    engine = create_db_engine(DatabaseConfig(database_url=settings.database_url, echo=settings.db_echo))
    SessionLocal = create_session_factory(engine)
    return SessionLocal()


def _api_sports_client() -> ApiSportsClient:
    return ApiSportsClient(
        base_url=settings.api_sports_base_url,
        api_key=settings.require_api_sports_key(),
    )


@api_sports_app.command("games")
def ingest_games_cmd(
    provider_league_id: str = typer.Option(..., help="api-sports league id"),
    league_name: str = typer.Option(..., help="Human name to store (e.g., NFL)"),
    sport: SportEnum = typer.Option(SportEnum.NFL, help="Sport enum"),
    season_year: int = typer.Option(..., help="Season year (e.g., 2024)"),
    season_type: SeasonTypeEnum | None = typer.Option(None, help="PRE/REG/POST"),
) -> None:
    with _session() as session:
        client = _api_sports_client()
        count = ingest_games(
            session,
            client=client,
            provider_league_id=provider_league_id,
            league_name=league_name,
            sport=sport,
            season_year=season_year,
            season_type=season_type,
            store_payloads=settings.store_ingested_payloads,
        )
        session.commit()
    typer.echo(f"Ingested/updated games: {count}")


@api_sports_app.command("game-stats")
def ingest_game_stats_cmd(
    provider_game_id: str = typer.Option(..., help="api-sports game id"),
) -> None:
    with _session() as session:
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
    provider_league_id: str = typer.Option(..., help="api-sports league id"),
    league_name: str = typer.Option(..., help="Human name to store (e.g., NFL)"),
    sport: SportEnum = typer.Option(SportEnum.NFL, help="Sport enum"),
    season_year: int = typer.Option(..., help="Season year (e.g., 2024)"),
    season_type: SeasonTypeEnum | None = typer.Option(None, help="PRE/REG/POST"),
) -> None:
    with _session() as session:
        client = _api_sports_client()
        games_count, stats_rows = ingest_games_with_stats(
            session,
            client=client,
            provider_league_id=provider_league_id,
            league_name=league_name,
            sport=sport,
            season_year=season_year,
            season_type=season_type,
            store_payloads=settings.store_ingested_payloads,
        )
        session.commit()
    typer.echo(f"Ingested/updated games: {games_count}; upserted team_game_stats rows: {stats_rows}")
