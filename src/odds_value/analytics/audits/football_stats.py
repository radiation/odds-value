from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from odds_value.db.models.core.game import Game
from odds_value.db.models.core.season import Season
from odds_value.db.models.features.football_team_game_stats import FootballTeamGameStats
from odds_value.db.models.features.team_game_stats import TeamGameStats


@dataclass(frozen=True, slots=True)
class SeasonCounts:
    season: int  # season_id
    games: int
    stats_rows: int  # football team stats rows (should be games * 2)

    @property
    def expected_stats_rows(self) -> int:
        return self.games * 2

    @property
    def stats_delta(self) -> int:
        """Positive means extra rows; negative means missing rows."""
        return self.stats_rows - self.expected_stats_rows


def season_rollup(session: Session) -> list[SeasonCounts]:
    """
    Roll up by season_id.

    Counts:
    - games: distinct Game.id
    - stats_rows: count of FootballTeamGameStats rows via 1:1 TeamGameStats extension
      (counting FootballTeamGameStats.team_game_stats_id since there is no PK id column)
    """
    stmt = (
        select(
            Season.id.label("season_id"),
            Season.year.label("season_year"),
            func.count(Game.id.distinct()).label("games"),
            func.count(FootballTeamGameStats.team_game_stats_id).label("stats_rows"),
        )
        .join(Game, Game.season_id == Season.id)
        .outerjoin(TeamGameStats, TeamGameStats.game_id == Game.id)
        .outerjoin(
            FootballTeamGameStats,
            FootballTeamGameStats.team_game_stats_id == TeamGameStats.id,
        )
        .group_by(Season.id, Season.year)
        .order_by(Season.year)
    )

    rows = session.execute(stmt).all()
    return [
        SeasonCounts(
            season=season_id,
            games=games,
            stats_rows=stats_rows,
        )
        for season_id, season_year, games, stats_rows in rows
    ]


def games_with_bad_stats_count(session: Session) -> list[tuple[int, int, int]]:
    """Returns (game_id, season_id, stats_count) for games where stats_count != 2."""
    stmt = (
        select(
            Game.id,
            Game.season_id,
            func.count(FootballTeamGameStats.team_game_stats_id).label("stats_count"),
        )
        .outerjoin(TeamGameStats, TeamGameStats.game_id == Game.id)
        .outerjoin(
            FootballTeamGameStats,
            FootballTeamGameStats.team_game_stats_id == TeamGameStats.id,
        )
        .group_by(Game.id, Game.season_id)
        .having(func.count(FootballTeamGameStats.team_game_stats_id) != 2)
        .order_by(Game.season_id, Game.id)
    )
    rows = list(session.execute(stmt).all())
    return [tuple(r) for r in rows]


def games_missing_any_stats(session: Session) -> list[tuple[int, int, int]]:
    """
    Returns (game_id, season_id, stats_count) for games where stats_count == 0.
    This is a subset of games_with_bad_stats_count(), but useful as a direct call.
    """
    stmt = (
        select(
            Game.id,
            Game.season_id,
            func.count(FootballTeamGameStats.team_game_stats_id).label("stats_count"),
        )
        .outerjoin(TeamGameStats, TeamGameStats.game_id == Game.id)
        .outerjoin(
            FootballTeamGameStats,
            FootballTeamGameStats.team_game_stats_id == TeamGameStats.id,
        )
        .group_by(Game.id, Game.season_id)
        .having(func.count(FootballTeamGameStats.team_game_stats_id) == 0)
        .order_by(Game.season_id, Game.id)
    )
    rows = list(session.execute(stmt).all())
    return [tuple(r) for r in rows]


# ----------------------------
# Breakdown helpers (where did it fail?)
# ----------------------------


def games_missing_team_game_stats(session: Session) -> list[tuple[int, int]]:
    """
    Returns (game_id, season_id) for games that have *zero* TeamGameStats rows.
    If this is non-empty, your ingestion never created team rows for those games.
    """
    stmt = (
        select(Game.id, Game.season_id)
        .outerjoin(TeamGameStats, TeamGameStats.game_id == Game.id)
        .where(TeamGameStats.id.is_(None))
        .order_by(Game.season_id, Game.id)
    )
    rows = list(session.execute(stmt).all())
    return [tuple(r) for r in rows]


def team_game_stats_missing_football_extension(session: Session) -> list[tuple[int, int]]:
    """
    Returns (team_game_stats_id, game_id) where the TeamGameStats row exists
    but the 1:1 FootballTeamGameStats extension row does not.
    """
    stmt = (
        select(TeamGameStats.id, TeamGameStats.game_id)
        .outerjoin(
            FootballTeamGameStats,
            FootballTeamGameStats.team_game_stats_id == TeamGameStats.id,
        )
        .where(FootballTeamGameStats.team_game_stats_id.is_(None))
        .order_by(TeamGameStats.game_id, TeamGameStats.id)
    )
    rows = list(session.execute(stmt).all())
    return [tuple(r) for r in rows]
