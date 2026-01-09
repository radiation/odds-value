from __future__ import annotations

import typer

from odds_value.cli.common import session_scope
from odds_value.ingestion.nflverse.nflverse_upsert import backfill_nflverse_team_stats

app = typer.Typer(help="Backfill NFL stats from nflverse/nfl_data_py.")


@app.command("team-stats")
def team_stats(
    from_year: int = typer.Option(2002, "--from-year"),
    to_year: int = typer.Option(2015, "--to-year"),
    dry_run: bool = typer.Option(True, "--dry-run/--write"),
) -> None:
    """
    Backfill FootballTeamGameStats for seasons where API-Sports has no coverage.
    """
    typer.echo(f"dry_run: {dry_run}")
    with session_scope() as session:
        result = backfill_nflverse_team_stats(
            session,
            from_year=from_year,
            to_year=to_year,
            dry_run=dry_run,
        )

    typer.echo("nflverse team stats backfill:")
    typer.echo(f"  years: {from_year}-{to_year}")
    typer.echo(f"  dry_run: {dry_run}")
    typer.echo(f"  games_considered: {result.games_considered}")
    typer.echo(f"  games_matched: {result.games_matched}")
    typer.echo(f"  team_rows_upserted: {result.team_rows_upserted}")
    typer.echo(f"  games_missing_schedule_match: {result.games_missing_schedule_match}")
    typer.echo(f"  team_rows_missing_stats: {result.team_rows_missing_stats}")

    if not dry_run and (
        result.games_missing_schedule_match > 0 or result.team_rows_missing_stats > 0
    ):
        typer.echo("  NOTE: Some games/teams did not match or had missing stats.")
