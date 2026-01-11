from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import Float, and_, cast, func, select
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement

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

    # features
    matchup_edge_l3_l5: float | None
    season_strength_pg: float | None
    league_avg_pts_season_to_date: float | None
    off_yards_edge_l3_l5: float | None
    turnover_edge_l3_l5: float | None


def shrink(value: float, games_played: int, k: float = 6.0) -> float:
    w = games_played / (games_played + k)
    return w * value


def shrink_sql(
    value: Any,
    games_played: Any,
    k: float,
) -> ColumnElement[float]:
    """SQL version of shrink(): w * value, where w=gp/(gp+k).

    Typed loosely because SQLAlchemy stubs model Labels / InstrumentedAttributes
    differently across versions.
    """
    gp = func.coalesce(games_played, 0)
    gp_f = cast(gp, Float)
    w = gp_f / (gp_f + float(k))
    v = cast(func.coalesce(value, 0.0), Float)
    return w * v


def z(x: Any) -> Any:
    return func.coalesce(x, 0)


def build_training_rows_stmt() -> Select[tuple[Any, ...]]:
    """
    One row per game with joined home/away TeamGameState (pre-game belief).
    Filters to games with final scores and matching TeamGameState rows.
    """
    home = aliased(TeamGameState)
    away = aliased(TeamGameState)

    games_min = func.least(home.games_played, away.games_played)

    matchup_edge_raw = (home.off_pts_l3 - away.def_pa_l5) - (away.off_pts_l3 - home.def_pa_l5)
    matchup_edge_l3_l5 = shrink_sql(matchup_edge_raw, games_min, k=3.0).label("matchup_edge_l3_l5")

    # Offensive yardage edge (ability to gain yards)
    off_yards_edge_l3_l5 = (home.off_yards_l3 - away.def_yards_allowed_l5) - (
        away.off_yards_l3 - home.def_yards_allowed_l5
    ).label("off_yards_edge_l3_l5")

    turnover_edge_raw = (z(away.off_turnovers_l3) - z(home.def_takeaways_l5)) - (
        z(home.off_turnovers_l3) - z(away.def_takeaways_l5)
    )

    turnover_edge_shrunk = shrink_sql(turnover_edge_raw, games_min, k=3.0)

    turnover_edge_l3_l5 = func.least(
        4.0,
        func.greatest(-4.0, turnover_edge_shrunk),
    ).label("turnover_edge_l3_l5")

    """
    season_strength = (
        ((home.off_pts_season - home.def_pa_season) - (away.off_pts_season - away.def_pa_season))
        / home.games_played
    ).label("season_strength")
    """

    denom_home = func.nullif(cast(home.games_played, Float), 0.0)
    denom_away = func.nullif(cast(away.games_played, Float), 0.0)

    home_avg_points_for = (cast(home.off_pts_season, Float) / denom_home).label(
        "home_avg_points_for"
    )
    home_avg_points_against = (cast(home.def_pa_season, Float) / denom_home).label(
        "home_avg_points_against"
    )
    home_avg_point_diff = (home_avg_points_for - home_avg_points_against).label(
        "home_avg_point_diff"
    )

    away_avg_points_for = (cast(away.off_pts_season, Float) / denom_away).label(
        "away_avg_points_for"
    )
    away_avg_points_against = (cast(away.def_pa_season, Float) / denom_away).label(
        "away_avg_points_against"
    )
    away_avg_point_diff = (away_avg_points_for - away_avg_points_against).label(
        "away_avg_point_diff"
    )

    home_strength_pg = home_avg_point_diff
    away_strength_pg = away_avg_point_diff

    home_strength_shrunk = shrink_sql(home_strength_pg, home.games_played, k=6.0)
    away_strength_shrunk = shrink_sql(away_strength_pg, away.games_played, k=6.0)

    season_strength = home_strength_shrunk - away_strength_shrunk

    g2 = aliased(Game)

    league_avg_pts_season_to_date = (
        select(func.avg(cast(g2.home_score + g2.away_score, Float)))
        .where(
            g2.season_id == Game.season_id,
            g2.start_time < Game.start_time,
            g2.home_score.is_not(None),
            g2.away_score.is_not(None),
        )
        .correlate(Game)
        .scalar_subquery()
    ).label("league_avg_pts_season_to_date")

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
            home_avg_points_for,
            home_avg_points_against,
            home_avg_point_diff,
            away_avg_points_for,
            away_avg_points_against,
            away_avg_point_diff,
            # Target
            (cast(Game.home_score, Float) - cast(Game.away_score, Float)).label("point_diff"),
            # Raw feature columns
            home.off_pts_l3.label("home_off_pts_l3"),
            home.def_pa_l5.label("home_def_pa_l5"),
            away.off_pts_l3.label("away_off_pts_l3"),
            away.def_pa_l5.label("away_def_pa_l5"),
            # Feature baseline input
            matchup_edge_l3_l5.label("matchup_edge_l3_l5"),
            season_strength.label("season_strength"),
            league_avg_pts_season_to_date.label("league_avg_pts_season_to_date"),
            off_yards_edge_l3_l5.label("off_yards_edge_l3_l5"),
            turnover_edge_l3_l5.label("turnover_edge_l3_l5"),
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
            season_strength_pg=r["season_strength"],
            league_avg_pts_season_to_date=float(r["league_avg_pts_season_to_date"]),
            off_yards_edge_l3_l5=r["off_yards_edge_l3_l5"],
            turnover_edge_l3_l5=r["turnover_edge_l3_l5"],
        )
        for r in rows
    ]
