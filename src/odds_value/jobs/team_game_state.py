from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any
from typing import cast as tcast

from sqlalchemy import Float, and_, cast, delete, func, select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from odds_value.db.models import Game, TeamGameState
from odds_value.db.models.features.football_team_game_stats import FootballTeamGameStats
from odds_value.db.models.features.team_game_stats import TeamGameStats


def backfill_team_game_state_avg_points(session: Session) -> None:
    tgs = TeamGameState.__table__
    denom = func.nullif(cast(tgs.c.games_played, Float), 0.0)

    stmt = (
        update(tcast(Any, tgs))
        .values(
            avg_points_for=cast(tgs.c.off_pts_season, Float) / denom,
            avg_points_against=cast(tgs.c.def_pa_season, Float) / denom,
            avg_point_diff=(
                (cast(tgs.c.off_pts_season, Float) / denom)
                - (cast(tgs.c.def_pa_season, Float) / denom)
            ),
        )
        .where(tgs.c.games_played > 0)
    )

    session.execute(stmt)
    session.commit()


def backfill_team_game_state_football_rollups(session: Session) -> None:
    tgs = TeamGameStats.__table__
    ftgs = FootballTeamGameStats.__table__
    g = Game.__table__
    tgs_opp = tgs.alias("tgs_opp")
    ftgs_opp = ftgs.alias("ftgs_opp")
    tgs_state = TeamGameState.__table__

    # base CTE: one row per team-game with opponent stats attached
    base = (
        select(
            g.c.id.label("game_id"),
            g.c.season_id.label("season_id"),
            g.c.start_time.label("start_time"),
            tgs.c.team_id.label("team_id"),
            ftgs.c.yards_total.label("off_yards"),
            ftgs.c.turnovers.label("off_turnovers"),
            ftgs_opp.c.yards_total.label("def_yards_allowed"),
            ftgs_opp.c.turnovers.label("def_takeaways"),
        )
        .select_from(tgs)
        .join(ftgs, ftgs.c.team_game_stats_id == tgs.c.id)
        .join(g, g.c.id == tgs.c.game_id)
        .join(
            tgs_opp,
            and_(
                tgs_opp.c.game_id == tgs.c.game_id,
                tgs_opp.c.team_id != tgs.c.team_id,
            ),
        )
        .join(ftgs_opp, ftgs_opp.c.team_game_stats_id == tgs_opp.c.id)
        .cte("base")
    )

    def wsum(col: ColumnElement[Any], n: int | None) -> ColumnElement[float]:
        """
        n=None -> season-to-date (unbounded preceding), always excluding current row (1 preceding)
        n=3/5 -> rolling windows excluding current row
        """
        frame = (-n, -1) if n else (None, -1)

        return func.coalesce(
            func.sum(col).over(
                partition_by=(base.c.team_id, base.c.season_id),
                order_by=base.c.start_time,
                rows=frame,
            ),
            0.0,
        )

    roll = select(
        base.c.game_id,
        base.c.team_id,
        wsum(base.c.off_yards, 3).label("off_yards_l3"),
        wsum(base.c.off_yards, 5).label("off_yards_l5"),
        wsum(base.c.off_yards, None).label("off_yards_season"),
        wsum(base.c.off_turnovers, 3).label("off_turnovers_l3"),
        wsum(base.c.off_turnovers, 5).label("off_turnovers_l5"),
        wsum(base.c.off_turnovers, None).label("off_turnovers_season"),
        wsum(base.c.def_yards_allowed, 3).label("def_yards_allowed_l3"),
        wsum(base.c.def_yards_allowed, 5).label("def_yards_allowed_l5"),
        wsum(base.c.def_yards_allowed, None).label("def_yards_allowed_season"),
        wsum(base.c.def_takeaways, 3).label("def_takeaways_l3"),
        wsum(base.c.def_takeaways, 5).label("def_takeaways_l5"),
        wsum(base.c.def_takeaways, None).label("def_takeaways_season"),
    ).cte("roll")

    stmt = (
        update(tcast(Any, tgs_state))
        .where(
            and_(
                tgs_state.c.game_id == roll.c.game_id,
                tgs_state.c.team_id == roll.c.team_id,
            )
        )
        .values(
            off_yards_l3=roll.c.off_yards_l3,
            off_yards_l5=roll.c.off_yards_l5,
            off_yards_season=roll.c.off_yards_season,
            off_turnovers_l3=roll.c.off_turnovers_l3,
            off_turnovers_l5=roll.c.off_turnovers_l5,
            off_turnovers_season=roll.c.off_turnovers_season,
            def_yards_allowed_l3=roll.c.def_yards_allowed_l3,
            def_yards_allowed_l5=roll.c.def_yards_allowed_l5,
            def_yards_allowed_season=roll.c.def_yards_allowed_season,
            def_takeaways_l3=roll.c.def_takeaways_l3,
            def_takeaways_l5=roll.c.def_takeaways_l5,
            def_takeaways_season=roll.c.def_takeaways_season,
        )
    )

    session.execute(stmt)
    session.commit()


@dataclass(frozen=True)
class _TeamResult:
    points_for: int
    points_against: int

    @property
    def point_diff(self) -> int:
        return self.points_for - self.points_against


def _compute_state(
    window: deque[_TeamResult],
) -> tuple[int, float | None, float | None, float | None]:
    """Return (games_played, avg_for, avg_against, avg_diff)."""
    n = len(window)
    if n == 0:
        return (0, None, None, None)

    total_for = 0
    total_against = 0
    total_diff = 0

    for r in window:
        total_for += r.points_for
        total_against += r.points_against
        total_diff += r.point_diff

    return (
        n,
        total_for / n,
        total_against / n,
        total_diff / n,
    )


def _avg_points_for(results: deque[_TeamResult]) -> float:
    if not results:
        return 0.0
    return sum(r.points_for for r in results) / len(results)


def _avg_points_against(results: deque[_TeamResult]) -> float:
    if not results:
        return 0.0
    return sum(r.points_against for r in results) / len(results)


def backfill_team_game_state(
    session: Session,
    *,
    league_id: int | None = None,
    season_id: int | None = None,
    commit_every_games: int = 250,
) -> int:
    """
    Build TeamGameState for all games matching filters.
    Creates 2 rows per game (home + away), using only games BEFORE each game.
    """
    stmt = select(Game).order_by(Game.start_time.asc(), Game.id.asc())

    if league_id is not None:
        stmt = stmt.where(Game.league_id == league_id)

    if season_id is not None:
        stmt = stmt.where(Game.season_id == season_id)

    games: list[Game] = list(session.scalars(stmt).all())

    if not games:
        return 0

    game_ids_subq = select(Game.id)

    if league_id is not None:
        game_ids_subq = game_ids_subq.where(Game.league_id == league_id)

    if season_id is not None:
        game_ids_subq = game_ids_subq.where(Game.season_id == season_id)

    del_stmt = delete(TeamGameState).where(TeamGameState.game_id.in_(game_ids_subq))
    session.execute(del_stmt)
    session.commit()

    last3: dict[int, deque[_TeamResult]] = defaultdict(lambda: deque(maxlen=3))
    last5: dict[int, deque[_TeamResult]] = defaultdict(lambda: deque(maxlen=5))

    # season aggregates keyed by (team_id, season_id)
    season_tot_pf: dict[tuple[int, int], int] = defaultdict(int)
    season_tot_pa: dict[tuple[int, int], int] = defaultdict(int)
    season_cnt: dict[tuple[int, int], int] = defaultdict(int)

    inserted = 0
    buffer: list[TeamGameState] = []

    for idx, g in enumerate(games, start=1):
        if g.home_score is None or g.away_score is None:
            continue

        home_key = (g.home_team_id, g.season_id)
        home_games_played = season_cnt[home_key]

        home_last3 = last3[g.home_team_id]
        home_last5 = last5[g.home_team_id]

        home_off_l3 = _avg_points_for(home_last3)
        home_def_l3 = _avg_points_against(home_last3)

        home_off_l5 = _avg_points_for(home_last5)
        home_def_l5 = _avg_points_against(home_last5)

        home_key = (g.home_team_id, g.season_id)
        home_off_season = float(season_tot_pf[home_key])
        home_def_season = float(season_tot_pa[home_key])

        buffer.append(
            TeamGameState(
                team_id=g.home_team_id,
                game_id=g.id,
                season_id=g.season_id,
                week=g.week,
                start_time=g.start_time,
                games_played=home_games_played,
                off_pts_l3=home_off_l3,
                def_pa_l3=home_def_l3,
                off_pts_l5=home_off_l5,
                def_pa_l5=home_def_l5,
                off_pts_season=home_off_season,
                def_pa_season=home_def_season,
            )
        )

        away_key = (g.away_team_id, g.season_id)
        away_games_played = season_cnt[away_key]

        away_last3 = last3[g.away_team_id]
        away_last5 = last5[g.away_team_id]

        away_off_l3 = _avg_points_for(away_last3)
        away_def_l3 = _avg_points_against(away_last3)
        away_off_l5 = _avg_points_for(away_last5)
        away_def_l5 = _avg_points_against(away_last5)

        away_key = (g.away_team_id, g.season_id)
        away_off_season = float(season_tot_pf[away_key])
        away_def_season = float(season_tot_pa[away_key])

        buffer.append(
            TeamGameState(
                team_id=g.away_team_id,
                game_id=g.id,
                season_id=g.season_id,
                week=g.week,
                start_time=g.start_time,
                games_played=away_games_played,
                off_pts_l3=away_off_l3,
                def_pa_l3=away_def_l3,
                off_pts_l5=away_off_l5,
                def_pa_l5=away_def_l5,
                off_pts_season=away_off_season,
                def_pa_season=away_def_season,
            )
        )

        last3[g.home_team_id].append(
            _TeamResult(points_for=g.home_score, points_against=g.away_score)
        )
        last5[g.home_team_id].append(
            _TeamResult(points_for=g.home_score, points_against=g.away_score)
        )

        last3[g.away_team_id].append(
            _TeamResult(points_for=g.away_score, points_against=g.home_score)
        )
        last5[g.away_team_id].append(
            _TeamResult(points_for=g.away_score, points_against=g.home_score)
        )

        season_tot_pf[home_key] += g.home_score
        season_tot_pa[home_key] += g.away_score
        season_cnt[home_key] += 1

        season_tot_pf[away_key] += g.away_score
        season_tot_pa[away_key] += g.home_score
        season_cnt[away_key] += 1

        if idx % commit_every_games == 0:
            session.add_all(buffer)
            session.commit()
            inserted += len(buffer)
            buffer.clear()

    if buffer:
        session.add_all(buffer)
        session.commit()
        inserted += len(buffer)

    backfill_team_game_state_avg_points(session)
    backfill_team_game_state_football_rollups(session)

    return inserted
