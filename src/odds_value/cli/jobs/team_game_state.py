from __future__ import annotations

from typing import Annotated

import typer

from odds_value.analytics.baseline import run_baseline_point_diff
from odds_value.cli.common import session_scope
from odds_value.jobs.team_game_state import backfill_team_game_state

team_game_state_app = typer.Typer(no_args_is_help=True)


@team_game_state_app.command("backfill")
def backfill(
    league_id: Annotated[int | None, typer.Option(help="Optional league id filter")] = None,
    season_id: Annotated[int | None, typer.Option(help="Optional season id filter")] = None,
    commit_every_games: Annotated[int, typer.Option(help="Commit every N games")] = 250,
) -> None:
    """
    Backfill TeamGameState (pre-game rolling belief state) for games in chronological order.
    """
    with session_scope() as session:
        inserted = backfill_team_game_state(
            session,
            league_id=league_id,
            season_id=season_id,
            commit_every_games=commit_every_games,
        )
        session.commit()

    typer.echo(f"Inserted TeamGameState rows: {inserted}")


@team_game_state_app.command("baseline")
def baseline(
    model_kind: str = "ridge",
    train_season_cutoff: int = 2021,
) -> None:
    """
    Run a simple train/test baseline on point_diff using diff_avg_point_diff.
    """
    with session_scope() as session:
        result = run_baseline_point_diff(
            session,
            model_kind=model_kind,
            train_season_cutoff=train_season_cutoff,
        )

    typer.echo(result)
