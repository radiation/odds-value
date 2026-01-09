from __future__ import annotations

from dataclasses import dataclass

import pandas as pd  # type: ignore


def norm_pbp_abbr(abbr: str, season_year: int) -> str:
    a = abbr.strip().upper()

    # minor aliases
    a = {"WSH": "WAS", "JAC": "JAX"}.get(a, a)

    # Rams: sometimes LA appears in older pbp
    if season_year <= 2015 and a in {"LA", "LAR"}:
        return "STL"

    # Chargers / Raiders: pbp can show modern codes even for old seasons
    if season_year <= 2016 and a == "LAC":
        return "SD"

    if season_year <= 2019 and a == "LV":
        return "OAK"

    return a


def to_nflverse_abbr(db_abbr: str, season_year: int) -> str:
    a = db_abbr.strip().upper()

    # Washington variations (nflverse often uses WAS; sometimes WSH)
    if a in {"WAS", "WSH"}:
        return "WAS"

    # Jacksonville variations (nflverse uses JAX)
    if a in {"JAX", "JAC"}:
        return "JAX"

    # Era-aware relocation mapping:
    if season_year <= 2015:
        return {
            "LAR": "STL",  # Rams were STL through 2015
            "LV": "OAK",  # Raiders were OAK through 2019
            "LAC": "SD",  # Chargers were SD through 2016
        }.get(a, a)

    if season_year == 2016:
        # Rams moved to LA in 2016; Chargers still SD in 2016
        return {
            "LV": "OAK",
            "LAC": "SD",
        }.get(a, a)

    if season_year <= 2019:
        # Raiders still OAK through 2019; Chargers are LAC from 2017+
        return {
            "LV": "OAK",
        }.get(a, a)

    return a


@dataclass(frozen=True, slots=True)
class ScheduleKey:
    season_year: int
    week: int
    home_abbr: str
    away_abbr: str


def build_schedule_index(schedules: pd.DataFrame) -> dict[ScheduleKey, str]:
    """
    Build mapping:
      (season_year, week, home_abbr, away_abbr) -> nflverse game_id
    Only includes regular season games.
    """
    required = {"season", "week", "game_type", "home_team", "away_team", "game_id"}
    missing = required - set(schedules.columns)
    if missing:
        raise ValueError(f"Schedules missing columns: {sorted(missing)}")

    df = schedules.copy()
    df = df[df["game_type"] == "REG"].copy()

    df["home_team"] = df.apply(
        lambda row: norm_pbp_abbr(str(row["home_team"]), int(row["season"])), axis=1
    )
    df["away_team"] = df.apply(
        lambda row: norm_pbp_abbr(str(row["away_team"]), int(row["season"])), axis=1
    )

    idx: dict[ScheduleKey, str] = {}
    for row in df.itertuples(index=False):
        key = ScheduleKey(
            season_year=int(row.season),
            week=int(row.week),
            home_abbr=norm_pbp_abbr(str(row.home_team), int(row.season)),
            away_abbr=norm_pbp_abbr(str(row.away_team), int(row.season)),
        )
        idx[key] = str(row.game_id)

    return idx


@dataclass(frozen=True, slots=True)
class TeamGameAgg:
    game_id: str  # nflverse game_id
    team_abbr: str
    yards_total: int
    turnovers: int


def aggregate_team_game_stats_from_pbp(pbp: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate play-by-play to per-team-per-game:
      yards_total = sum(yards_gained)
      turnovers = sum(interception) + sum(fumble_lost)
    Returns DataFrame columns:
      game_id, team_abbr, yards_total, turnovers
    """
    required = {"season", "game_id", "posteam", "yards_gained", "interception", "fumble_lost"}
    missing = required - set(pbp.columns)
    if missing:
        raise ValueError(f"PBP missing columns: {sorted(missing)}")

    df = pbp.copy()
    df = df[df["posteam"].notna()].copy()

    df["posteam"] = df.apply(
        lambda row: norm_pbp_abbr(str(row["posteam"]), int(row["season"])), axis=1
    )

    for c in ["yards_gained", "interception", "fumble_lost"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    grouped = df.groupby(["season", "game_id", "posteam"], as_index=False).agg(
        yards_total=("yards_gained", "sum"),
        interceptions=("interception", "sum"),
        fumbles_lost=("fumble_lost", "sum"),
    )

    grouped["turnovers"] = grouped["interceptions"] + grouped["fumbles_lost"]

    out = grouped.rename(columns={"posteam": "team_abbr"})[
        ["game_id", "team_abbr", "yards_total", "turnovers"]
    ].copy()

    # cast to python ints
    out["yards_total"] = out["yards_total"].round(0).astype(int)
    out["turnovers"] = out["turnovers"].round(0).astype(int)

    return out


def build_team_game_stats_lookup(
    team_game_stats: pd.DataFrame,
) -> dict[tuple[str, str], tuple[int, int]]:
    """
    Build lookup:
      (nflverse_game_id, team_abbr) -> (yards_total, turnovers)
    """
    required = {"game_id", "team_abbr", "yards_total", "turnovers"}
    missing = required - set(team_game_stats.columns)
    if missing:
        raise ValueError(f"Aggregated stats missing columns: {sorted(missing)}")

    lookup: dict[tuple[str, str], tuple[int, int]] = {}
    for row in team_game_stats.itertuples(index=False):
        lookup[(str(row.game_id), str(row.team_abbr))] = (int(row.yards_total), int(row.turnovers))
    return lookup


__all__ = [
    "aggregate_team_game_stats_from_pbp",
    "build_schedule_index",
    "norm_pbp_abbr",
    "to_nflverse_abbr",
]
