from __future__ import annotations

from typing import Annotated

import typer

from odds_value.cli.common import session_scope
from odds_value.jobs.team_game_state import backfill_team_game_state

team_game_state_app = typer.Typer(no_args_is_help=True)


@team_game_state_app.command("backfill")
def backfill(
    league_id: Annotated[int | None, typer.Option(help="Optional league id filter")] = None,
    season_id: Annotated[int | None, typer.Option(help="Optional season id filter")] = None,
    window_size: Annotated[int, typer.Option(help="Rolling window size (last N games)")] = 5,
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
            window_size=window_size,
            commit_every_games=commit_every_games,
        )
        session.commit()

    typer.echo(f"Inserted TeamGameState rows: {inserted}")
