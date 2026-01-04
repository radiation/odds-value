from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from odds_value.db.models import Game, TeamGameState


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
        home_off_season = (
            season_tot_pf[home_key] / season_cnt[home_key] if season_cnt[home_key] else 0.0
        )
        home_def_season = (
            season_tot_pa[home_key] / season_cnt[home_key] if season_cnt[home_key] else 0.0
        )

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
        away_off_season = (
            season_tot_pf[away_key] / season_cnt[away_key] if season_cnt[away_key] else 0.0
        )
        away_def_season = (
            season_tot_pa[away_key] / season_cnt[away_key] if season_cnt[away_key] else 0.0
        )

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

    return inserted
