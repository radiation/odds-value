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


def backfill_team_game_state(
    session: Session,
    *,
    league_id: int | None = None,
    season_id: int | None = None,
    window_size: int = 5,
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

    del_stmt = (
        delete(TeamGameState)
        .where(TeamGameState.game_id.in_(game_ids_subq))
        .where(TeamGameState.window_size == window_size)
    )
    session.execute(del_stmt)
    session.commit()

    windows: dict[int, deque[_TeamResult]] = defaultdict(lambda: deque(maxlen=window_size))

    inserted = 0
    buffer: list[TeamGameState] = []

    for idx, g in enumerate(games, start=1):
        if g.home_score is None or g.away_score is None:
            continue

        home_window = windows[g.home_team_id]
        home_games_played, home_avg_for, home_avg_against, home_avg_diff = _compute_state(
            home_window
        )

        buffer.append(
            TeamGameState(
                team_id=g.home_team_id,
                game_id=g.id,
                season_id=g.season_id,
                week=g.week,
                start_time=g.start_time,
                games_played=home_games_played,
                window_size=window_size,
                avg_points_for=home_avg_for,
                avg_points_against=home_avg_against,
                avg_point_diff=home_avg_diff,
            )
        )

        away_window = windows[g.away_team_id]
        away_games_played, away_avg_for, away_avg_against, away_avg_diff = _compute_state(
            away_window
        )

        buffer.append(
            TeamGameState(
                team_id=g.away_team_id,
                game_id=g.id,
                season_id=g.season_id,
                week=g.week,
                start_time=g.start_time,
                games_played=away_games_played,
                window_size=window_size,
                avg_points_for=away_avg_for,
                avg_points_against=away_avg_against,
                avg_point_diff=away_avg_diff,
            )
        )

        windows[g.home_team_id].append(
            _TeamResult(points_for=g.home_score, points_against=g.away_score)
        )
        windows[g.away_team_id].append(
            _TeamResult(points_for=g.away_score, points_against=g.home_score)
        )

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
