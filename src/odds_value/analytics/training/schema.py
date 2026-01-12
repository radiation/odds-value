from dataclasses import dataclass


@dataclass(frozen=True)
class BaselineResult:
    model_name: str
    model_mae: float
    model_rmse: float
    zero_mae: float
    zero_rmse: float
    home_mean_mae: float
    home_mean_rmse: float
    coef: dict[str, float] | None
    intercept: float | None


@dataclass(frozen=True)
class GameTrainingRow:
    game_id: int
    start_time: object
    season_id: int
    season_year: int
    week: int

    home_team_id: int
    away_team_id: int

    # target
    point_diff: int

    # raw belief features
    home_avg_points_for: float | None
    home_avg_points_against: float | None
    home_avg_point_diff: float | None

    away_avg_points_for: float | None
    away_avg_points_against: float | None
    away_avg_point_diff: float | None

    # features
    matchup_edge_l3_l5: float | None
    season_strength_pg: float | None
    league_avg_pts_season_to_date: float | None
    off_yards_edge_l3_l5: float | None
    turnover_edge_l3_l5: float | None
