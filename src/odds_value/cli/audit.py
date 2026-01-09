from __future__ import annotations

from collections import Counter

import typer
from sqlalchemy import select

from odds_value.analytics.audits.football_stats import (
    games_missing_any_stats,
    games_missing_team_game_stats,
    games_with_bad_stats_count,
    season_rollup,
    team_game_stats_missing_football_extension,
)
from odds_value.cli.common import session_scope
from odds_value.db.models.core.season import Season

app = typer.Typer(help="Data integrity audits.")


@app.command("football-stats")
def football_stats() -> None:
    """Audit that NFL games have 2 team stats rows and that seasons look sane."""
    with session_scope() as session:
        rollups = season_rollup(session)
        season_years = {
            sid: year for sid, year in session.execute(select(Season.id, Season.year)).all()
        }
        missing = games_missing_any_stats(session)
        bad = games_with_bad_stats_count(session)

    typer.echo("Season rollup (games vs stats_rows):")
    for r in rollups:
        year = season_years.get(r.season, "??")
        ok = "OK" if r.stats_rows == r.expected_stats_rows else "BAD"
        typer.echo(
            f"  season_id={r.season} year={year}: "
            f"games={r.games} stats_rows={r.stats_rows} "
            f"(expected {r.expected_stats_rows}) [{ok}]"
        )

    # --- NEW: broken games by season summary (top 10) ---
    typer.echo("")
    typer.echo("Broken games by season (stats_count != 2):")
    by_season = Counter(season_id for _, season_id, _ in bad)
    for season_id, n in by_season.most_common(10):
        year = season_years.get(season_id, "??")
        typer.echo(f"  season={season_id} year={year}: {n}")

    typer.echo("")
    typer.echo(f"Games missing stats rows: {len(missing)}")
    for game_id, season_id, _cnt in missing[:20]:
        typer.echo(f"  season={season_id} game_id={game_id}")
    if len(missing) > 20:
        typer.echo("  ...")

    typer.echo("")
    typer.echo(f"Games with stats_count != 2: {len(bad)}")
    for game_id, season_id, cnt in bad[:20]:
        typer.echo(f"  season={season_id} game_id={game_id} stats_count={cnt}")
    if len(bad) > 20:
        typer.echo("  ...")

    # --- NEW: highlight broken games outside season 4 (first 50) ---
    typer.echo("")
    typer.echo("Broken games outside season 4 (first 50):")
    outside_season_4 = [t for t in bad if t[1] != 4]
    for game_id, season_id, cnt in outside_season_4[:50]:
        typer.echo(f"  season={season_id} game_id={game_id} stats_count={cnt}")
    if len(outside_season_4) > 50:
        typer.echo("  ...")

    # Diagnose where it broke
    typer.echo("")
    typer.echo("Breakdown: missing TeamGameStats vs missing FootballTeamGameStats extension")

    with session_scope() as session:
        missing_tgs = games_missing_team_game_stats(session)
        missing_ext = team_game_stats_missing_football_extension(session)

    # Summarize counts
    typer.echo(f"Games missing TeamGameStats rows: {len(missing_tgs)}")
    typer.echo(f"TeamGameStats rows missing FootballTeamGameStats extension: {len(missing_ext)}")

    # Focused: season 4 + the two specific games
    season4_missing_tgs = [(gid, sid) for gid, sid in missing_tgs if sid == 4]
    typer.echo(f"Season 4 games missing TeamGameStats: {len(season4_missing_tgs)}")

    focus_game_ids = {2442, 5612}
    focus_missing_tgs = [(gid, sid) for gid, sid in missing_tgs if gid in focus_game_ids]
    typer.echo(f"Focus games missing TeamGameStats: {focus_missing_tgs}")

    focus_missing_ext = [
        (tgs_id, game_id) for tgs_id, game_id in missing_ext if game_id in focus_game_ids
    ]
    typer.echo(
        f"Focus games missing FootballTeamGameStats extension rows: {focus_missing_ext[:10]}"
    )

    # Optional: non-zero exit for CI
    if missing or bad:
        raise typer.Exit(code=1)
