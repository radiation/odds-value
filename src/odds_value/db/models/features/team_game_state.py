from __future__ import annotations

from datetime import datetime

from sqlalchemy import ForeignKey, Index, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from odds_value.db.base import Base, TimestampMixin


class TeamGameState(Base, TimestampMixin):
    __tablename__ = "team_game_state"

    id: Mapped[int] = mapped_column(primary_key=True)

    team_id: Mapped[int] = mapped_column(ForeignKey("teams.id"), nullable=False)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.id"), nullable=False)

    start_time: Mapped[datetime] = mapped_column(nullable=False)
    season_id: Mapped[int] = mapped_column(ForeignKey("seasons.id"), nullable=False)
    week: Mapped[int] = mapped_column(Integer, nullable=False)

    games_played: Mapped[int] = mapped_column(Integer, nullable=False)
    window_size: Mapped[int] = mapped_column(Integer, nullable=False)

    avg_points_for: Mapped[float | None]
    avg_points_against: Mapped[float | None]
    avg_point_diff: Mapped[float | None]

    team: Mapped[Team] = relationship("Team")
    game: Mapped[Game] = relationship("Game")
    season: Mapped[Season] = relationship("Season")

    __table_args__ = (
        UniqueConstraint("team_id", "game_id", name="uq_team_game_state_team_game"),
        UniqueConstraint(
            "team_id",
            "game_id",
            "window_size",
            name="uq_team_game_state_team_game_window",
        ),
        Index("ix_team_game_state_team_start_time", "team_id", "start_time"),
        Index("ix_team_game_state_game", "game_id"),
        Index("ix_team_game_state_season_week", "season_id", "week"),
        Index("ix_team_game_state_team_window", "team_id", "window_size", "start_time"),
    )


from odds_value.db.models.core.game import Game  # noqa: E402
from odds_value.db.models.core.season import Season  # noqa: E402
from odds_value.db.models.core.team import Team  # noqa: E402
