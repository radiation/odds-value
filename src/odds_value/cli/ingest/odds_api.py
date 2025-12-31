from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated

import typer
from sqlalchemy.orm import Session

from odds_value.core.config import settings
from odds_value.db import DatabaseConfig, create_db_engine, create_session_factory
from odds_value.db.enums import SportEnum
from odds_value.ingestion.odds_api.odds import ingest_odds
from odds_value.ingestion.odds_api.odds_api_client import OddsApiClient

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


def _parse_dt_utc(value: str) -> datetime:
    """
    Parse CLI datetime:
      - '2024-09-07T20:20:00Z'
      - '2024-09-07T20:20:00+00:00'
      - '2024-09-07T20:20:00' (assumed UTC)
      - '2024-09-07' (assumed 00:00 UTC)
    """
    s = value.strip()
    if not s:
        raise typer.BadParameter("Datetime cannot be empty.")

    if "T" not in s:
        s = f"{s}T00:00:00"

    s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

    return dt.astimezone(UTC)


@odds_api_app.command("odds")
def ingest_odds_cmd(
    sport: Annotated[SportEnum, typer.Option(help="Sport enum")] = SportEnum.NFL,
    days_ahead: Annotated[
        int,
        typer.Option(
            help="How many days ahead to fetch odds for (current endpoint returns upcoming games)"
        ),
    ] = 7,
    regions: Annotated[str, typer.Option(help="Odds API regions parameter (e.g. us, eu)")] = "us",
    markets: Annotated[
        str, typer.Option(help="Markets to fetch (h2h, spreads, totals)")
    ] = "h2h,spreads,totals",
) -> None:
    """
    Ingest main-line odds (moneyline, spreads, totals) from Odds API (current/upcoming).
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
            snapshot_at=None,  # <-- current mode
        )

        session.commit()

    typer.echo(f"Inserted odds snapshots: {inserted}")


@odds_api_app.command("odds-backfill")
def backfill_odds_cmd(
    sport: Annotated[SportEnum, typer.Option(help="Sport enum")] = SportEnum.NFL,
    start: Annotated[
        str, typer.Option(help="UTC start datetime/date (e.g. 2021-09-01 or 2021-09-01T00:00:00Z)")
    ] = "2021-01-01",
    end: Annotated[str, typer.Option(help="UTC end datetime/date (inclusive)")] = "2025-12-31",
    step_minutes: Annotated[
        int, typer.Option(help="Minutes between snapshot requests (e.g. 1440=daily, 60=hourly)")
    ] = 1440,
    regions: Annotated[str, typer.Option(help="Odds API regions parameter (e.g. us, eu)")] = "us",
    markets: Annotated[
        str, typer.Option(help="Markets to fetch (h2h, spreads, totals)")
    ] = "h2h,spreads,totals",
    commit_every: Annotated[
        int, typer.Option(help="Commit every N snapshots (1 = commit each snapshot)")
    ] = 1,
    store_payloads: Annotated[
        bool, typer.Option(help="Store ingested payloads (can explode storage for backfills)")
    ] = False,
) -> None:
    """
    Backfill historical odds snapshots from Odds API over a time range.

    Example:
      odds-api odds-backfill --start 2021-09-01 --end 2022-02-15 --step-minutes 1440
    """
    start_dt = _parse_dt_utc(start)
    end_dt = _parse_dt_utc(end)

    if end_dt < start_dt:
        raise typer.BadParameter("--end must be >= --start")

    if step_minutes <= 0:
        raise typer.BadParameter("--step-minutes must be > 0")

    with _session() as session:
        client = _odds_api_client()

        t = start_dt
        i = 0
        total_inserted = 0

        while t <= end_dt:
            inserted = ingest_odds(
                session,
                client=client,
                sport=sport,
                regions=regions,
                markets=markets,
                days_ahead=0,  # unused in historical mode
                store_payloads=store_payloads,
                snapshot_at=t,
            )
            total_inserted += inserted
            i += 1

            if i % commit_every == 0:
                session.commit()
                typer.echo(f"[{t.isoformat()}] inserted={inserted} total={total_inserted}")

            t = t + timedelta(minutes=step_minutes)

        # final commit if commit_every > 1 and we ended mid-batch
        session.commit()

    typer.echo(f"Backfill complete. Total odds snapshots inserted: {total_inserted}")
