from __future__ import annotations

import typer

from odds_value.cli.ingest import ingest_app

app = typer.Typer(no_args_is_help=True)
app.add_typer(ingest_app, name="ingest")
