from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import and_, select
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

    # matchup deltas
    diff_avg_points_for: float | None
    diff_avg_points_against: float | None
    diff_avg_point_diff: float | None


def build_training_rows_stmt(*, window_size: int) -> Select[tuple[Any, ...]]:
    """
    One row per game with joined home/away TeamGameState (pre-game belief).
    Filters to games with final scores and matching TeamGameState rows.
    """
    home = aliased(TeamGameState)
    away = aliased(TeamGameState)

    stmt = (
        select(
            Game.id.label("game_id"),
            Game.start_time.label("start_time"),
            Game.season_id.label("season_id"),
            Season.year.label("season_year"),
            Game.week.label("week"),
            Game.home_team_id.label("home_team_id"),
            Game.away_team_id.label("away_team_id"),
            # target
            (Game.home_score - Game.away_score).label("point_diff"),
            # belief stats
            home.games_played.label("home_games_played"),
            away.games_played.label("away_games_played"),
            home.avg_points_for.label("home_avg_points_for"),
            home.avg_points_against.label("home_avg_points_against"),
            home.avg_point_diff.label("home_avg_point_diff"),
            away.avg_points_for.label("away_avg_points_for"),
            away.avg_points_against.label("away_avg_points_against"),
            away.avg_point_diff.label("away_avg_point_diff"),
            # deltas
            (home.avg_point_diff - away.avg_point_diff).label("diff_avg_point_diff"),
            (home.avg_points_for - away.avg_points_for).label("diff_avg_points_for"),
            (home.avg_points_against - away.avg_points_against).label("diff_avg_points_against"),
        )
        .join(
            home,
            and_(
                home.game_id == Game.id,
                home.team_id == Game.home_team_id,
            ),
        )
        .join(
            away,
            and_(
                away.game_id == Game.id,
                away.team_id == Game.away_team_id,
            ),
        )
        .join(Season, Season.id == Game.season_id)
        .where(Game.home_score.is_not(None))
        .where(Game.away_score.is_not(None))
        .order_by(Game.start_time.asc(), Game.id.asc())
    )

    return stmt


def fetch_training_rows(
    session: Session, *, window_size: int, limit: int = 25
) -> list[GameTrainingRow]:
    stmt = build_training_rows_stmt(window_size=window_size).limit(limit)
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
            diff_avg_points_for=r["diff_avg_points_for"],
            diff_avg_points_against=r["diff_avg_points_against"],
            diff_avg_point_diff=r["diff_avg_point_diff"],
        )
        for r in rows
    ]
