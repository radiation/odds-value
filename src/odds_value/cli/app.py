from __future__ import annotations

import typer

from odds_value.cli.audit import app as audit_app
from odds_value.cli.ingest.api_sports import api_sports_app
from odds_value.cli.ingest.nflverse import app as nflverse_app
from odds_value.cli.ingest.odds_api import odds_api_app
from odds_value.cli.jobs.team_game_state import team_game_state_app

app = typer.Typer(no_args_is_help=True)
app.add_typer(audit_app, name="audit")
app.add_typer(api_sports_app, name="api-sports")
app.add_typer(nflverse_app, name="nflverse")
app.add_typer(odds_api_app, name="odds-api")
app.add_typer(team_game_state_app, name="team-game-state")
