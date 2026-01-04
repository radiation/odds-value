from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import Float, and_, cast, select
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import Select

from odds_value.db.models import Game, Season, TeamGameState


@dataclass(frozen=True)
class GameTrainingRow:
    game_id: int
    start_time: object
    season_id: int
    week: int

    home_team_id: int
    away_team_id: int

    # target
    point_diff: int

    # raw belief features
    home_avg_points_for: float | None
    home_avg_points_against: float | None
    home_avg_point_diff: float | None

    away_avg_points_for: float | None
    away_avg_points_against: float | None
    away_avg_point_diff: float | None

    # difference features
    matchup_edge_l3_l5: float | None


def build_training_rows_stmt() -> Select[tuple[Any, ...]]:
    """
    One row per game with joined home/away TeamGameState (pre-game belief).
    Filters to games with final scores and matching TeamGameState rows.
    """
    home = aliased(TeamGameState)
    away = aliased(TeamGameState)

    matchup_edge_l3_l5 = (
        (home.off_pts_l3 - away.def_pa_l5) - (away.off_pts_l3 - home.def_pa_l5)
    ).label("matchup_edge_l3_l5")

    matchup_edge_season = (
        (home.off_pts_season - away.def_pa_season) - (away.off_pts_season - home.def_pa_season)
    ).label("matchup_edge_season")

    stmt = (
        select(
            Game.id.label("game_id"),
            Game.start_time.label("start_time"),
            Game.season_id.label("season_id"),
            Season.year.label("season_year"),
            Game.week.label("week"),
            Game.home_team_id.label("home_team_id"),
            Game.away_team_id.label("away_team_id"),
            home.games_played.label("home_games_played"),
            away.games_played.label("away_games_played"),
            # Target
            (cast(Game.home_score, Float) - cast(Game.away_score, Float)).label("point_diff"),
            # Raw feature columns (optional but useful for debugging)
            home.off_pts_l3.label("home_off_pts_l3"),
            home.def_pa_l5.label("home_def_pa_l5"),
            away.off_pts_l3.label("away_off_pts_l3"),
            away.def_pa_l5.label("away_def_pa_l5"),
            # Feature baseline input
            matchup_edge_l3_l5,
            matchup_edge_season,
        )
        .select_from(Game)
        .join(Season, Season.id == Game.season_id)
        .join(home, and_(home.team_id == Game.home_team_id, home.game_id == Game.id))
        .join(away, and_(away.team_id == Game.away_team_id, away.game_id == Game.id))
        .where(Game.home_score.is_not(None))
        .where(Game.away_score.is_not(None))
        .order_by(Game.start_time.asc(), Game.id.asc())
    )

    return stmt


def fetch_training_rows(session: Session, *, limit: int = 25) -> list[GameTrainingRow]:
    stmt = build_training_rows_stmt().limit(limit)
    rows = session.execute(stmt).mappings().all()

    return [
        GameTrainingRow(
            game_id=r["game_id"],
            start_time=r["start_time"],
            season_id=r["season_id"],
            week=r["week"],
            home_team_id=r["home_team_id"],
            away_team_id=r["away_team_id"],
            point_diff=r["point_diff"],
            home_avg_points_for=r["home_avg_points_for"],
            home_avg_points_against=r["home_avg_points_against"],
            home_avg_point_diff=r["home_avg_point_diff"],
            away_avg_points_for=r["away_avg_points_for"],
            away_avg_points_against=r["away_avg_points_against"],
            away_avg_point_diff=r["away_avg_point_diff"],
            matchup_edge_l3_l5=r["matchup_edge_l3_l5"],
        )
        for r in rows
    ]
