from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from odds_value.db.base import Base, TimestampMixin
from odds_value.db.enums import GameStatusEnum


class Game(Base, TimestampMixin):
    __tablename__ = "games"

    id: Mapped[int] = mapped_column(primary_key=True)

    league_id: Mapped[int] = mapped_column(
        ForeignKey("leagues.id", ondelete="CASCADE"), nullable=False
    )
    season_id: Mapped[int | None] = mapped_column(
        ForeignKey("seasons.id", ondelete="SET NULL"), nullable=True
    )

    provider_game_id: Mapped[str] = mapped_column(String, nullable=False, unique=True)

    start_time: Mapped[datetime] = mapped_column(nullable=False)

    venue_id: Mapped[int | None] = mapped_column(
        ForeignKey("venues.id", ondelete="SET NULL"), nullable=True
    )

    status: Mapped[GameStatusEnum] = mapped_column(nullable=False, default=GameStatusEnum.UNKNOWN)
    week: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_neutral_site: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )

    home_team_id: Mapped[int] = mapped_column(
        ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )
    away_team_id: Mapped[int] = mapped_column(
        ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )

    home_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    away_score: Mapped[int | None] = mapped_column(Integer, nullable=True)

    source_last_seen_at: Mapped[datetime | None] = mapped_column(nullable=True)

    league: Mapped[League] = relationship(back_populates="games")
    season: Mapped[Season | None] = relationship(back_populates="games")

    venue: Mapped[Venue | None] = relationship(back_populates="games")

    home_team: Mapped[Team] = relationship(back_populates="home_games", foreign_keys=[home_team_id])
    away_team: Mapped[Team] = relationship(back_populates="away_games", foreign_keys=[away_team_id])

    team_stats: Mapped[list[TeamGameStats]] = relationship(
        back_populates="game",
        cascade="all, delete-orphan",
    )

    odds_snapshots: Mapped[list[OddsSnapshot]] = relationship(
        back_populates="game",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_games_league_start_time", "league_id", "start_time"),
        Index("ix_games_season_week", "season_id", "week"),
        Index("ix_games_home_team", "home_team_id"),
        Index("ix_games_away_team", "away_team_id"),
        Index("ix_games_venue_start_time", "venue_id", "start_time"),
    )


from odds_value.db.models.league import League  # noqa: E402
from odds_value.db.models.odds_snapshot import OddsSnapshot  # noqa: E402
from odds_value.db.models.season import Season  # noqa: E402
from odds_value.db.models.team import Team  # noqa: E402
from odds_value.db.models.team_game_stats import TeamGameStats  # noqa: E402
from odds_value.db.models.venue import Venue  # noqa: E402
