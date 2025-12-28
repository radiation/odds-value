from __future__ import annotations

from typing import Any

from sqlalchemy import JSON, Boolean, ForeignKey, Index, Integer, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from odds_value.db.base import Base, TimestampMixin


class TeamGameStats(Base, TimestampMixin):
    __tablename__ = "team_game_stats"

    id: Mapped[int] = mapped_column(primary_key=True)

    game_id: Mapped[int] = mapped_column(ForeignKey("games.id", ondelete="CASCADE"), nullable=False)
    team_id: Mapped[int] = mapped_column(ForeignKey("teams.id", ondelete="CASCADE"), nullable=False)
    is_home: Mapped[bool] = mapped_column(Boolean, nullable=False)

    stats_json: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=False,
    )

    # Convenience fields (optional but useful)
    points: Mapped[int | None] = mapped_column(Integer, nullable=True)
    yards_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    turnovers: Mapped[int | None] = mapped_column(Integer, nullable=True)

    game: Mapped[Game] = relationship(back_populates="team_stats")
    team: Mapped[Team] = relationship(back_populates="team_game_stats")

    __table_args__ = (
        UniqueConstraint("game_id", "team_id", name="uq_team_game_stats_game_team"),
        Index("ix_team_game_stats_team_game", "team_id", "game_id"),
    )


from odds_value.db.models.game import Game  # noqa: E402
from odds_value.db.models.team import Team  # noqa: E402
