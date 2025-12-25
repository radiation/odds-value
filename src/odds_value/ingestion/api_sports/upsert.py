from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from odds_value.db.enums import SeasonTypeEnum, SportEnum
from odds_value.db.models import Game, IngestedPayload, League, Season, Team, TeamGameStats, Venue
from odds_value.ingestion.api_sports.mappers import (
    coerce_int, map_game_status, parse_week, stats_list_to_map,
)
from odds_value.ingestion.api_sports.dates import (
    compute_week_from_start_time_nfl,
    parse_api_sports_game_datetime,
)


def upsert_league(session: Session, *, provider_league_id: str, name: str, sport: SportEnum) -> League:
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
    season_type: SeasonTypeEnum | None = None,
    name: str | None = None,
    is_active: bool | None = None,
) -> Season:
    stmt = select(Season).where(
        Season.league_id == league_id,
        Season.year == year,
        Season.season_type == season_type,
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
        season_type=season_type,
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
    city: str | None = None,
    nickname: str | None = None,
) -> Team:
    stmt = select(Team).where(Team.league_id == league_id, Team.provider_team_id == provider_team_id)
    team = session.scalar(stmt)
    if team:
        team.name = name
        if logo_url is not None:
            team.logo_url = logo_url
        if abbreviation is not None:
            team.abbreviation = abbreviation
        if city is not None:
            team.city = city
        if nickname is not None:
            team.nickname = nickname
        return team

    team = Team(
        league_id=league_id,
        provider_team_id=provider_team_id,
        name=name,
        logo_url=logo_url,
        abbreviation=abbreviation,
        city=city,
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


def upsert_game_from_api_sports_item(
    session: Session,
    *,
    league: League,
    season: Season | None,
    item: dict[str, Any],
    source_last_seen_at: datetime,
) -> Game:
    game_obj = item.get("game") or item.get("fixture") or {}
    teams_obj = item.get("teams") or {}
    scores_obj = item.get("scores") or {}

    provider_game_id = str(game_obj.get("id"))
    start_time = parse_api_sports_game_datetime(
        game_obj.get("date"),
        provider_game_id=provider_game_id,
    )

    status_short = None
    status_obj = game_obj.get("status")
    if isinstance(status_obj, dict):
        status_short = status_obj.get("short")

    home = teams_obj.get("home") or {}
    away = teams_obj.get("away") or {}

    home_provider_id = str(home.get("id"))
    away_provider_id = str(away.get("id"))

    home_team = upsert_team(
        session,
        league_id=league.id,
        provider_team_id=home_provider_id,
        name=str(home.get("name") or ""),
        logo_url=home.get("logo"),
    )
    away_team = upsert_team(
        session,
        league_id=league.id,
        provider_team_id=away_provider_id,
        name=str(away.get("name") or ""),
        logo_url=away.get("logo"),
    )

    venue_id: int | None = None
    venue_obj = game_obj.get("venue")
    if isinstance(venue_obj, dict):
        provider_venue_id = venue_obj.get("id")
        name = str(venue_obj.get("name") or "")
        city = venue_obj.get("city")

        if name:
            venue = upsert_venue(
                session,
                league_id=league.id,
                provider_venue_id=str(provider_venue_id) if provider_venue_id is not None else None,
                name=name,
                city=city,
            )
            venue_id = venue.id

    home_score = None
    away_score = None
    if isinstance(scores_obj, dict):
        h = scores_obj.get("home") or {}
        a = scores_obj.get("away") or {}
        if isinstance(h, dict):
            home_score = coerce_int(h.get("total"))
        if isinstance(a, dict):
            away_score = coerce_int(a.get("total"))

    week_raw = game_obj.get("week")
    week = parse_week(week_raw)

    if week is None and league.sport == SportEnum.NFL:
        week = compute_week_from_start_time_nfl(start_time, season_year=season.year)

    game = session.scalar(select(Game).where(Game.provider_game_id == provider_game_id))
    if game:
        game.league_id = league.id
        game.season_id = season.id if season else None
        game.start_time = start_time
        game.venue_id = venue_id
        game.status = map_game_status(status_short)
        game.week = week
        game.home_team_id = home_team.id
        game.away_team_id = away_team.id
        game.home_score = home_score
        game.away_score = away_score
        game.source_last_seen_at = source_last_seen_at
        return game

    game = Game(
        league_id=league.id,
        season_id=season.id if season else None,
        provider_game_id=provider_game_id,
        start_time=start_time,
        venue_id=venue_id,
        status=map_game_status(status_short),
        week=week,
        is_neutral_site=False,
        home_team_id=home_team.id,
        away_team_id=away_team.id,
        home_score=home_score,
        away_score=away_score,
        source_last_seen_at=source_last_seen_at,
    )
    session.add(game)
    session.flush()
    return game


def upsert_team_game_stats(
    session: Session,
    *,
    game: Game,
    team: Team,
    is_home: bool,
    stats: list[dict[str, Any]] | None,
) -> TeamGameStats:
    stats_map = stats_list_to_map(stats)

    yards_total = coerce_int(stats_map.get("Total Yards"))
    turnovers = coerce_int(stats_map.get("Turnovers"))

    points = game.home_score if is_home else game.away_score

    stmt = select(TeamGameStats).where(
        TeamGameStats.game_id == game.id,
        TeamGameStats.team_id == team.id,
    )
    row = session.scalar(stmt)
    if row:
        row.is_home = is_home
        row.stats_json = stats_map
        row.points = points
        row.yards_total = yards_total
        row.turnovers = turnovers
        return row

    row = TeamGameStats(
        game_id=game.id,
        team_id=team.id,
        is_home=is_home,
        stats_json=stats_map,
        points=points,
        yards_total=yards_total,
        turnovers=turnovers,
    )
    session.add(row)
    session.flush()
    return row


def maybe_store_payload(
    session: Session,
    *,
    enabled: bool,
    provider: str,
    entity_type: str,
    entity_key: str,
    fetched_at: datetime,
    payload: dict[str, Any],
) -> None:
    if not enabled:
        return

    session.add(
        IngestedPayload(
            provider=provider,
            entity_type=entity_type,
            entity_key=entity_key,
            fetched_at=fetched_at,
            payload_json=payload,
        )
    )
