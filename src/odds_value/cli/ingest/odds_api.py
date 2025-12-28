from __future__ import annotations

import typer
from sqlalchemy.orm import Session

from odds_value.core.config import settings
from odds_value.db import DatabaseConfig, create_db_engine, create_session_factory
from odds_value.db.enums import SportEnum
from odds_value.ingestion.odds_api.client import OddsApiClient
from odds_value.ingestion.odds_api.odds import ingest_odds

odds_api_app = typer.Typer(no_args_is_help=True)


def _session() -> Session:
    engine = create_db_engine(
        DatabaseConfig(
            database_url=settings.database_url,
            echo=settings.db_echo,
        )
    )
    SessionLocal = create_session_factory(engine)
    return SessionLocal()


def _odds_api_client() -> OddsApiClient:
    return OddsApiClient(
        base_url=settings.odds_api_base_url,
        api_key=settings.require_odds_api_key(),
    )


@odds_api_app.command("odds")
def ingest_odds_cmd(
    sport: SportEnum = typer.Option(SportEnum.NFL, help="Sport enum"),
    days_ahead: int = typer.Option(
        7,
        help="How many days ahead to fetch odds for (Odds API returns upcoming games)",
    ),
    regions: str = typer.Option(
        "us",
        help="Odds API regions parameter (e.g. us, eu)",
    ),
    markets: str = typer.Option(
        "h2h,spreads,totals",
        help="Markets to fetch (h2h, spreads, totals)",
    ),
) -> None:
    """
    Ingest main-line odds (moneyline, spreads, totals) from Odds API.

    This command:
    - Resolves Odds API events to existing canonical games
    - Upserts books
    - Inserts odds_snapshots (idempotent via unique indexes)
    """

    with _session() as session:
        client = _odds_api_client()

        inserted = ingest_odds(
            session,
            client=client,
            sport=sport,
            regions=regions,
            markets=markets,
            days_ahead=days_ahead,
            store_payloads=settings.store_ingested_payloads,
        )

        session.commit()

    typer.echo(f"Inserted odds snapshots: {inserted}")
