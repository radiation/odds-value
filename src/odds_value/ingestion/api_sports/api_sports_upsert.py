from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from odds_value.db.enums import ProviderEnum, SportEnum
from odds_value.db.models import Game, IngestedPayload, League, Season, Team, TeamGameStats, Venue
from odds_value.db.models.features.baseball_team_game_stats import BaseballTeamGameStats
from odds_value.db.models.features.football_team_game_stats import FootballTeamGameStats
from odds_value.ingestion.api_sports.api_sports_mappers import (
    coerce_int,
    map_game_status,
    stats_list_to_map,
)
from odds_value.ingestion.common.dates import (
    compute_week_from_start_time_nfl,
    parse_api_sports_game_datetime,
    parse_nfl_week,
)
from odds_value.ingestion.common.utils import none_if_empty
from odds_value.repos.games_repo import upsert_game


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


def upsert_game_from_api_sports_item(
    session: Session,
    *,
    league: League,
    season: Season | None,
    item: dict[str, Any],
    source_last_seen_at: datetime,
) -> Game | None:
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

    home_name = none_if_empty(home.get("name"))
    away_name = none_if_empty(away.get("name"))

    if not home_name or not away_name:
        raise ValueError(f"Missing team name in api-sports payload: game_id={provider_game_id}")

    home_team = upsert_team(
        session,
        league_id=league.id,
        provider_team_id=home_provider_id,
        name=home_name,
        logo_url=none_if_empty(home.get("logo")),
    )
    away_team = upsert_team(
        session,
        league_id=league.id,
        provider_team_id=away_provider_id,
        name=away_name,
        logo_url=none_if_empty(away.get("logo")),
    )

    venue_obj = game_obj.get("venue")
    venue: Venue | None = None
    if isinstance(venue_obj, dict):
        provider_venue_id = venue_obj.get("id")
        name = none_if_empty(venue_obj.get("name"))
        city = none_if_empty(venue_obj.get("city"))

        if name:
            venue = upsert_venue(
                session,
                league_id=league.id,
                provider_venue_id=str(provider_venue_id) if provider_venue_id is not None else None,
                name=name,
                city=city,
            )

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
    week = parse_nfl_week(week_raw)

    if league.sport == SportEnum.NFL and season and start_time:
        computed = compute_week_from_start_time_nfl(start_time, season_year=season.year)
        if computed is not None:
            if week is not None and week != computed:
                print(
                    f"Overriding NFL week from provider with computed week: provider_week={week}, computed_week={computed}, provider_game_id={provider_game_id}"
                )
            week = computed
        else:
            return None

    if season is None:
        return None

    game = upsert_game(
        session=session,
        league=league,
        season=season,
        provider=ProviderEnum.API_SPORTS,
        provider_game_id=provider_game_id,
        start_time=start_time,
        home_team=home_team,
        away_team=away_team,
        venue=venue,
        status=map_game_status(status_short),
        week=week,
        is_neutral_site=False,
        home_score=home_score,
        away_score=away_score,
        source_last_seen_at=source_last_seen_at,
    )

    return game


def upsert_team_game_stats(
    session: Session,
    *,
    game: Game,
    team: Team,
    is_home: bool,
    stats: list[dict[str, Any]] | None,
) -> TeamGameStats:
    # Base row
    score = game.home_score if is_home else game.away_score

    base_stmt = select(TeamGameStats).where(
        TeamGameStats.game_id == game.id,
        TeamGameStats.team_id == team.id,
    )
    base = session.scalar(base_stmt)
    if base:
        base.is_home = is_home
        base.score = score
    else:
        base = TeamGameStats(
            game_id=game.id,
            team_id=team.id,
            is_home=is_home,
            score=score,
        )
        session.add(base)
        session.flush()

    # Determine sport
    sport = session.scalar(select(League.sport).where(League.id == game.league_id))
    if sport is None:
        return base

    # Sport-specific extension row
    stats_map = stats_list_to_map(stats)  # returns dict[str, Any]

    def get_dict(d: dict[str, Any], key: str) -> dict[str, Any]:
        v = d.get(key)
        return v if isinstance(v, dict) else {}

    if sport == SportEnum.NFL:
        yards_total = coerce_int(get_dict(stats_map, "yards").get("total"))
        turnovers = coerce_int(get_dict(stats_map, "turnovers").get("total"))

        nfl_ext = session.get(FootballTeamGameStats, base.id)
        if nfl_ext:
            nfl_ext.yards_total = yards_total
            nfl_ext.turnovers = turnovers
            nfl_ext.stats_json = stats_map
        else:
            session.add(
                FootballTeamGameStats(
                    team_game_stats_id=base.id,
                    yards_total=yards_total,
                    turnovers=turnovers,
                    stats_json=stats_map,
                )
            )

    elif sport == SportEnum.MLB:
        hits = coerce_int(get_dict(stats_map, "hits").get("total"))
        errors = coerce_int(get_dict(stats_map, "errors").get("total"))

        mlb_ext = session.get(BaseballTeamGameStats, base.id)
        if mlb_ext:
            mlb_ext.hits = hits
            mlb_ext.errors = errors
            mlb_ext.stats_json = stats_map
        else:
            session.add(
                BaseballTeamGameStats(
                    team_game_stats_id=base.id,
                    hits=hits,
                    errors=errors,
                    stats_json=stats_map,
                )
            )

    # else: other sports no-op for now

    session.flush()
    return base


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
