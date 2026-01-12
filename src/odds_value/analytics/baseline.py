from __future__ import annotations

import math
from collections.abc import Sequence

import numpy as np
from numpy.typing import NDArray
from sklearn.ensemble import HistGradientBoostingRegressor  # type: ignore[import-untyped]
from sklearn.linear_model import RidgeCV  # type: ignore[import-untyped]
from sklearn.pipeline import Pipeline  # type: ignore[import-untyped]
from sklearn.preprocessing import StandardScaler  # type: ignore[import-untyped]
from sqlalchemy.orm import Session

from odds_value.analytics.training.schema import BaselineResult, GameTrainingRow
from odds_value.repos.training_data_repo import fetch_training_rows

ArrayF64 = NDArray[np.float64]


def run_baseline_point_diff(
    session: Session,
    *,
    train_season_cutoff: int,
    model_kind: str = "ridge",
) -> BaselineResult:
    rows: list[GameTrainingRow] = fetch_training_rows(session)

    # Split by season
    train = [r for r in rows if r.season_year < train_season_cutoff]
    test = [r for r in rows if r.season_year >= train_season_cutoff]

    if not train:
        raise ValueError("No training rows produced — check training_data filters / joins")

    if not test:
        raise ValueError("No test rows produced — check training_data filters / joins")

    def extract_xy(data: Sequence[GameTrainingRow]) -> tuple[ArrayF64, ArrayF64]:
        X = np.array(
            [
                [
                    r.matchup_edge_l3_l5,
                    r.off_yards_edge_l3_l5,
                    r.turnover_edge_l3_l5,
                    r.season_strength_pg,
                    r.league_avg_pts_season_to_date,
                ]
                for r in data
            ],
            dtype=float,
        )
        if not np.isfinite(X).all():
            bad = np.argwhere(~np.isfinite(X))
            i, j = bad[0]
            raise ValueError(
                f"Non-finite value in X at row {i}, col {j}: {X[i, j]!r}. "
                f"Row keys: matchup={data[i].matchup_edge_l3_l5}, "
                f"off_yards={data[i].off_yards_edge_l3_l5}, "
                f"to={data[i].turnover_edge_l3_l5}, "
                f"season_strength={data[i].season_strength_pg}, "
                f"league_avg={data[i].league_avg_pts_season_to_date}"
            )
        y = np.array([r.point_diff for r in data], dtype=float)

        return X, y

    X_train, y_train = extract_xy(train)
    X_test, y_test = extract_xy(test)

    # Baseline predictions
    zero_pred = np.zeros_like(y_test)
    home_mean = float(np.mean(y_train))
    home_pred = np.full_like(y_test, home_mean)

    FEATURE_NAMES = [
        "matchup_edge_l3_l5",
        "off_yards_edge_l3_l5",
        "turnover_edge_l3_l5",
        "season_strength_pg",
        "league_avg_pts_season_to_date",
    ]

    # Model
    if model_kind == "ridge":
        model = Pipeline(
            [
                ("scaler", StandardScaler()),
                ("model", RidgeCV(alphas=np.logspace(-2, 3, 30))),
            ]
        )

        model.fit(X_train, y_train)
        model_pred = model.predict(X_test)

        ridge: RidgeCV = model.named_steps["model"]
        coefs = ridge.coef_.tolist()
        coef_by_feature = dict(zip(FEATURE_NAMES, coefs, strict=False))
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
        coef=coef_by_feature if model_kind == "ridge" else None,
        intercept=intercept,
    )
