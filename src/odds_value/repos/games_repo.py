from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from odds_value.db.enums import GameStatusEnum, ProviderEnum
from odds_value.db.models import Game, League, Season, Team, Venue


def upsert_game(
    session: Session,
    *,
    league: League,
    season: Season,
    provider: ProviderEnum,
    provider_game_id: str,
    start_time: datetime,
    home_team: Team,
    away_team: Team,
    venue: Venue | None,
    status: GameStatusEnum,
    week: int | None,
    is_neutral_site: bool,
    home_score: int | None,
    away_score: int | None,
    source_last_seen_at: datetime,
) -> Game:
    game = session.scalar(
        select(Game).where(
            Game.provider_game_id == provider_game_id,
            Game.provider == provider,
        )
    )

    if game:
        game.league_id = league.id
        game.season_id = season.id
        game.start_time = start_time
        game.venue_id = venue.id if venue else None
        game.status = status
        game.week = week
        game.is_neutral_site = is_neutral_site
        game.home_team_id = home_team.id
        game.away_team_id = away_team.id
        game.home_score = home_score
        game.away_score = away_score
        game.source_last_seen_at = source_last_seen_at
        return game

    game = Game(
        league_id=league.id,
        season_id=season.id,
        provider_game_id=provider_game_id,
        start_time=start_time,
        venue_id=venue.id if venue else None,
        status=status,
        week=week,
        is_neutral_site=is_neutral_site,
        home_team_id=home_team.id,
        away_team_id=away_team.id,
        home_score=home_score,
        away_score=away_score,
        source_last_seen_at=source_last_seen_at,
    )
    session.add(game)
    session.flush()
    return game
