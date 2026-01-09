from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from odds_value.db.enums import SportEnum
from odds_value.db.models import Game, League, Season, Team
from odds_value.db.models.features.football_team_game_stats import FootballTeamGameStats
from odds_value.ingestion.api_sports.api_sports_upsert import upsert_team_game_stats
from odds_value.ingestion.nflverse.nflverse_client import NflverseClient
from odds_value.ingestion.nflverse.nflverse_transform import (
    ScheduleKey,
    aggregate_team_game_stats_from_pbp,
    build_schedule_index,
    build_team_game_stats_lookup,
)


@dataclass(frozen=True, slots=True)
class BackfillResult:
    games_considered: int
    games_matched: int
    team_rows_upserted: int
    games_missing_schedule_match: int
    team_rows_missing_stats: int


@dataclass(frozen=True, slots=True)
class StatsMissSample:
    game_id: int
    nflverse_game_id: str
    team_abbr: str
    season_year: int
    week: int
    present_team_abbrs: list[str]


def _stats_list(yards_total: int, turnovers: int) -> list[dict[str, object]]:
    """
    Shape must work with stats_list_to_map() in api_sports_upsert.py such that:
      stats_map["yards"]["total"] == yards_total
      stats_map["turnovers"]["total"] == turnovers
    """
    return [
        {"type": "yards", "total": int(yards_total)},
        {"type": "turnovers", "total": int(turnovers)},
    ]


def upsert_football_extension(
    session: Session,
    team_game_stats_id: int,
    *,
    yards_total: int | None,
    turnovers: int | None,
    stats_json: dict[str, object] | None = None,
) -> None:
    ext = (
        session.query(FootballTeamGameStats)
        .filter(FootballTeamGameStats.team_game_stats_id == team_game_stats_id)
        .one_or_none()
    )

    if ext is None:
        ext = FootballTeamGameStats(team_game_stats_id=team_game_stats_id)
        session.add(ext)

    ext.yards_total = yards_total
    ext.turnovers = turnovers
    if stats_json is not None:
        ext.stats_json = stats_json


def backfill_nflverse_team_stats(
    session: Session,
    *,
    from_year: int,
    to_year: int,
    dry_run: bool = True,
) -> BackfillResult:
    from odds_value.ingestion.nflverse.nflverse_transform import (
        to_nflverse_abbr,  # avoid circular import
    )

    years = list(range(from_year, to_year + 1))

    client = NflverseClient()

    schedules = client.import_schedules(years)
    schedule_index = build_schedule_index(schedules)

    # PBP aggregation
    pbp_cols = ["season", "game_id", "posteam", "yards_gained", "interception", "fumble_lost"]
    pbp = client.import_pbp(years, columns=pbp_cols)
    agg_df = aggregate_team_game_stats_from_pbp(pbp)
    stats_lookup = build_team_game_stats_lookup(agg_df)

    nfl_league_ids = session.scalars(select(League.id).where(League.sport == SportEnum.NFL)).all()

    # Pull games with season year + home/away team abbreviations
    stmt = (
        select(
            Game,
            Season.year,
            Team,  # home
            Team,  # away
        )
        .join(Season, Season.id == Game.season_id)
        .join(League, League.id == Game.league_id)
        .where(League.id.in_(nfl_league_ids))
        .where(Season.year >= from_year, Season.year <= to_year)
    )
    # Need proper aliases for the two Team joins
    from sqlalchemy.orm import aliased

    HomeTeam = aliased(Team)
    AwayTeam = aliased(Team)

    stmt = (
        select(Game, Season.year, HomeTeam, AwayTeam)
        .join(Season, Season.id == Game.season_id)
        .join(League, League.id == Game.league_id)
        .join(HomeTeam, HomeTeam.id == Game.home_team_id)
        .join(AwayTeam, AwayTeam.id == Game.away_team_id)
        .where(League.sport == SportEnum.NFL)
        .where(Season.year >= from_year, Season.year <= to_year)
    )

    rows = session.execute(stmt).all()

    games_considered = 0
    games_matched = 0
    team_rows_upserted = 0
    games_missing_schedule_match = 0
    team_rows_missing_stats = 0
    sample_schedule_misses: list[tuple[int, int, int, str, str]] = []
    sample_stats_misses: list[StatsMissSample] = []

    for game, season_year, home_team, away_team in rows:
        games_considered += 1

        if game.week is None:
            games_missing_schedule_match += 1
            continue

        if not home_team.abbreviation or not away_team.abbreviation:
            games_missing_schedule_match += 1
            continue

        home_abbr = to_nflverse_abbr(home_team.abbreviation, int(season_year))
        away_abbr = to_nflverse_abbr(away_team.abbreviation, int(season_year))

        key = ScheduleKey(
            season_year=int(season_year),
            week=int(game.week),
            home_abbr=home_abbr,
            away_abbr=away_abbr,
        )

        nflverse_game_id = schedule_index.get(key)
        if not nflverse_game_id:
            if len(sample_schedule_misses) < 25:
                sample_schedule_misses.append(
                    (
                        game.id,
                        int(season_year),
                        int(game.week),
                        home_abbr,
                        away_abbr,
                    )
                )
            games_missing_schedule_match += 1
            continue

        games_matched += 1

        # Two team rows per game
        for team, is_home in ((home_team, True), (away_team, False)):
            team_abbr = to_nflverse_abbr(team.abbreviation, int(season_year))

            stats = stats_lookup.get((nflverse_game_id, team_abbr))
            if not stats:
                team_rows_missing_stats += 1
                if len(sample_stats_misses) < 25:
                    present: list[str] = sorted(
                        {t for (gid, t) in stats_lookup if gid == nflverse_game_id}
                    )
                    sample_stats_misses.append(
                        StatsMissSample(
                            game.id,
                            nflverse_game_id,
                            team_abbr,
                            int(season_year),
                            int(game.week),
                            present,
                        )
                    )
                continue

            yards_total, turnovers = stats

            if dry_run:
                team_rows_upserted += 1
                continue

            tgs = upsert_team_game_stats(
                session,
                game=game,
                team=team,
                is_home=is_home,
                stats=None,
            )

            upsert_football_extension(
                session,
                tgs.id,
                yards_total=int(yards_total) if yards_total is not None else None,
                turnovers=int(turnovers) if turnovers is not None else None,
                stats_json={
                    "source": "nflverse",
                    "yards_total": int(yards_total) if yards_total is not None else None,
                    "turnovers": int(turnovers) if turnovers is not None else None,
                },
            )
            team_rows_upserted += 1

    if not dry_run:
        print("Committing nflverse team stats backfill...")
        session.commit()

    print("Sample schedule misses (game_id, season_year, week, home_abbr, away_abbr):")
    for sched_miss in sample_schedule_misses:
        print(sched_miss)

    print(
        "Sample stats misses (game_id, nflverse_game_id, team_abbr, season_year, week, present_team_abbrs):"
    )
    for sample_miss in sample_stats_misses:
        print(sample_miss)

    df = schedules[(schedules["season"] == 2010) & (schedules["week"] == 3)]
    print(df[["home_team", "away_team", "game_id"]].head(50))

    return BackfillResult(
        games_considered=games_considered,
        games_matched=games_matched,
        team_rows_upserted=team_rows_upserted,
        games_missing_schedule_match=games_missing_schedule_match,
        team_rows_missing_stats=team_rows_missing_stats,
    )
