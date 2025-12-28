import typer

from odds_value.cli.ingest.api_sports import api_sports_app
from odds_value.cli.ingest.odds_api import odds_api_app

ingest_app = typer.Typer(no_args_is_help=True)
ingest_app.add_typer(api_sports_app, name="api-sports")
ingest_app.add_typer(odds_api_app, name="odds-api")

