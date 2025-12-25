from __future__ import annotations

from decimal import Decimal
from typing import Optional

from sqlalchemy import ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from odds_value.db.base import Base, TimestampMixin
from odds_value.db.enums import RoofTypeEnum, SurfaceTypeEnum


class Venue(Base, TimestampMixin):
    __tablename__ = "venues"

    id: Mapped[int] = mapped_column(primary_key=True)

    league_id: Mapped[Optional[int]] = mapped_column(ForeignKey("leagues.id", ondelete="SET NULL"), nullable=True)
    provider_venue_id: Mapped[str | None] = mapped_column(String, nullable=True, unique=True)

    name: Mapped[str] = mapped_column(String, nullable=False)
    city: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    country: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    latitude: Mapped[Optional[Decimal]] = mapped_column(nullable=True)
    longitude: Mapped[Optional[Decimal]] = mapped_column(nullable=True)
    timezone: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    is_indoor: Mapped[Optional[bool]] = mapped_column(nullable=True)
    roof_type: Mapped[Optional[RoofTypeEnum]] = mapped_column(nullable=True)
    surface_type: Mapped[Optional[SurfaceTypeEnum]] = mapped_column(nullable=True)
    altitude_m: Mapped[Optional[int]] = mapped_column(nullable=True)

    league: Mapped[Optional["League"]] = relationship(back_populates="venues")
    games: Mapped[list["Game"]] = relationship(back_populates="venue")

    __table_args__ = (
        UniqueConstraint("league_id", "name", "city", name="uq_venues_league_name_city"),
        Index("ix_venues_league_name", "league_id", "name"),
        Index("ix_venues_lat_lon", "latitude", "longitude"),
    )


from odds_value.db.models.league import League  # noqa: E402
from odds_value.db.models.game import Game  # noqa: E402
