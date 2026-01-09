from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray
from sklearn.ensemble import HistGradientBoostingRegressor  # type: ignore[import-untyped]
from sklearn.linear_model import RidgeCV  # type: ignore[import-untyped]
from sklearn.pipeline import Pipeline  # type: ignore[import-untyped]
from sklearn.preprocessing import StandardScaler  # type: ignore[import-untyped]
from sqlalchemy import func
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from odds_value.analytics.training_data import build_training_rows_stmt

ArrayF64 = NDArray[np.float64]


@dataclass(frozen=True)
class BaselineResult:
    model_name: str
    model_mae: float
    model_rmse: float
    zero_mae: float
    zero_rmse: float
    home_mean_mae: float
    home_mean_rmse: float
    coef: float | None
    intercept: float | None


def run_baseline_point_diff(
    session: Session,
    *,
    train_season_cutoff: int,
    model_kind: str = "ridge",
) -> BaselineResult:
    # Pull rows
    stmt = build_training_rows_stmt()
    stmt = stmt.where(
        func.coalesce(stmt.selected_columns.home_games_played, 0) >= 3,
        func.coalesce(stmt.selected_columns.away_games_played, 0) >= 3,
    )
    rows = session.execute(stmt).mappings().all()

    # Split by season
    train = [r for r in rows if r["season_year"] < train_season_cutoff]
    test = [r for r in rows if r["season_year"] >= train_season_cutoff]

    if not train:
        raise ValueError("No training rows produced — check training_data filters / joins")

    if not test:
        raise ValueError("No test rows produced — check training_data filters / joins")

    def extract_xy(data: Sequence[RowMapping]) -> tuple[ArrayF64, ArrayF64]:
        X = np.array(
            [
                [
                    r["matchup_edge_l3_l5"],
                    r["season_strength"],
                    r["league_avg_pts_season_to_date"],
                ]
                for r in data
            ],
            dtype=float,
        )
        y = np.array([r["point_diff"] for r in data], dtype=float)
        return X, y

    X_train, y_train = extract_xy(train)
    X_test, y_test = extract_xy(test)

    # Baseline predictions
    zero_pred = np.zeros_like(y_test)
    home_mean = float(np.mean(y_train))
    home_pred = np.full_like(y_test, home_mean)

    # Model
    if model_kind == "ridge":
        model = Pipeline(
            [("scaler", StandardScaler()), ("model", RidgeCV(alphas=np.logspace(-3, 3, 25)))]
        )
        model.fit(X_train, y_train)
        model_pred = model.predict(X_test)

        ridge: RidgeCV = model.named_steps["model"]
        coef = float(ridge.coef_[0])
        intercept = float(ridge.intercept_)
        name = "ridgecv"
    else:
        model = HistGradientBoostingRegressor(
            max_depth=3,
            learning_rate=0.05,
            max_iter=600,
            min_samples_leaf=25,
            l2_regularization=0.0,
            early_stopping=True,
            validation_fraction=0.15,
            n_iter_no_change=30,
            random_state=42,
        )
        coef = None
        intercept = None
        name = "hgb_depth3_leaf25"

    model.fit(X_train, y_train)
    model_pred = model.predict(X_test)

    def mae(y: ArrayF64, yhat: ArrayF64) -> float:
        return float(np.mean(np.abs(y - yhat)))

    def rmse(y: ArrayF64, yhat: ArrayF64) -> float:
        return float(math.sqrt(np.mean((y - yhat) ** 2)))

    return BaselineResult(
        model_name=name,
        model_mae=mae(y_test, model_pred),
        model_rmse=rmse(y_test, model_pred),
        zero_mae=mae(y_test, zero_pred),
        zero_rmse=rmse(y_test, zero_pred),
        home_mean_mae=mae(y_test, home_pred),
        home_mean_rmse=rmse(y_test, home_pred),
        coef=coef,
        intercept=intercept,
    )
