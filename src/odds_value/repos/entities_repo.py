from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from odds_value.db.enums import SportEnum
from odds_value.db.models import League, Season, Team, Venue


def upsert_league(
    session: Session, *, provider_league_id: str, name: str, sport: SportEnum
) -> League:
    league = session.scalar(select(League).where(League.provider_league_id == provider_league_id))
    if league:
        league.name = name
        league.sport = sport
        return league

    league = League(provider_league_id=provider_league_id, name=name, sport=sport, is_active=True)
    session.add(league)
    session.flush()
    return league


def upsert_season(
    session: Session,
    *,
    league_id: int,
    year: int,
    name: str | None = None,
    is_active: bool | None = None,
) -> Season:
    stmt = select(Season).where(
        Season.league_id == league_id,
        Season.year == year,
    )
    season = session.scalar(stmt)
    if season:
        if name is not None:
            season.name = name
        if is_active is not None:
            season.is_active = is_active
        return season

    season = Season(
        league_id=league_id,
        year=year,
        name=name,
        is_active=bool(is_active) if is_active is not None else False,
    )
    session.add(season)
    session.flush()
    return season


def upsert_team(
    session: Session,
    *,
    league_id: int,
    provider_team_id: str,
    name: str,
    logo_url: str | None = None,
    abbreviation: str | None = None,
    market: str | None = None,
    nickname: str | None = None,
) -> Team:
    stmt = select(Team).where(
        Team.league_id == league_id, Team.provider_team_id == provider_team_id
    )
    team = session.scalar(stmt)
    if team:
        team.name = name
        if logo_url is not None:
            team.logo_url = logo_url
        if abbreviation is not None:
            team.abbreviation = abbreviation
        if market is not None:
            team.market = market
        if nickname is not None:
            team.nickname = nickname
        return team

    team = Team(
        league_id=league_id,
        provider_team_id=provider_team_id,
        name=name,
        logo_url=logo_url,
        abbreviation=abbreviation,
        market=market,
        nickname=nickname,
        is_active=True,
    )
    session.add(team)
    session.flush()
    return team


def upsert_venue(
    session: Session,
    *,
    league_id: int | None,
    provider_venue_id: str | None,
    name: str,
    city: str | None = None,
) -> Venue:
    venue: Venue | None = None

    if provider_venue_id:
        venue = session.scalar(select(Venue).where(Venue.provider_venue_id == provider_venue_id))

    if venue is None:
        venue = session.scalar(
            select(Venue).where(
                Venue.league_id == league_id,
                Venue.name == name,
                Venue.city == city,
            )
        )

    if venue:
        venue.name = name
        venue.city = city
        if venue.league_id is None and league_id is not None:
            venue.league_id = league_id
        if venue.provider_venue_id is None and provider_venue_id is not None:
            venue.provider_venue_id = provider_venue_id
        return venue

    venue = Venue(
        league_id=league_id,
        provider_venue_id=provider_venue_id,
        name=name,
        city=city,
    )
    session.add(venue)
    session.flush()
    return venue
