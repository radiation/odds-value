from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
from sklearn.linear_model import Ridge  # type: ignore[import-untyped]
from sqlalchemy import func
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from odds_value.analytics.training_data import build_training_rows_stmt


@dataclass(frozen=True)
class BaselineResult:
    model_mae: float
    model_rmse: float
    zero_mae: float
    zero_rmse: float
    home_mean_mae: float
    home_mean_rmse: float
    coef: float
    intercept: float


def run_baseline_point_diff(
    session: Session,
    *,
    window_size: int,
    min_games: int,
    train_season_cutoff: int,
) -> BaselineResult:
    # Pull rows
    stmt = build_training_rows_stmt(window_size=window_size)

    stmt = stmt.where(
        func.coalesce(stmt.selected_columns.home_games_played, 0) >= min_games,
        func.coalesce(stmt.selected_columns.away_games_played, 0) >= min_games,
    )

    rows = session.execute(stmt).mappings().all()
    print(f"Total training rows fetched: {len(rows)}")

    # Split by season
    train = [r for r in rows if r["season_year"] < train_season_cutoff]
    test = [r for r in rows if r["season_year"] >= train_season_cutoff]

    print(f"Training rows: {len(train)}, Test rows: {len(test)}")

    if not train:
        raise ValueError("No training rows produced — check training_data filters / joins")

    if not test:
        raise ValueError("No test rows produced — check training_data filters / joins")

    def extract_xy(data: Sequence[RowMapping]) -> tuple[np.ndarray, np.ndarray]:
        X = np.array([[r["diff_avg_point_diff"]] for r in data], dtype=float)
        y = np.array([r["point_diff"] for r in data], dtype=float)
        return X, y

    X_train, y_train = extract_xy(train)
    X_test, y_test = extract_xy(test)

    # Baseline predictions
    zero_pred = np.zeros_like(y_test)
    home_mean = float(np.mean(y_train))
    home_pred = np.full_like(y_test, home_mean)

    # Model
    model = Ridge(alpha=1.0)
    model.fit(X_train, y_train)
    model_pred = model.predict(X_test)

    def mae(y: np.ndarray, yhat: np.ndarray) -> float:
        return float(np.mean(np.abs(y - yhat)))

    def rmse(y: np.ndarray, yhat: np.ndarray) -> float:
        return float(math.sqrt(np.mean((y - yhat) ** 2)))

    return BaselineResult(
        model_mae=mae(y_test, model_pred),
        model_rmse=rmse(y_test, model_pred),
        zero_mae=mae(y_test, zero_pred),
        zero_rmse=rmse(y_test, zero_pred),
        home_mean_mae=mae(y_test, home_pred),
        home_mean_rmse=rmse(y_test, home_pred),
        coef=float(model.coef_[0]),
        intercept=float(model.intercept_),
    )
