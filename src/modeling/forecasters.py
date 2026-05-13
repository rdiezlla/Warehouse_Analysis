from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Ridge

from src.modeling.feature_policy import select_training_features, validate_feature_columns

@dataclass
class ForecasterConfig:
    family: str
    frequency: str
    lags: list[int]
    rolling_windows: list[int]
    random_state: int
    drop_features: tuple[str, ...] = ()


class RecursiveForecaster:
    def __init__(self, config: ForecasterConfig):
        self.config = config
        self.estimator = self._make_estimator()
        self.feature_columns: list[str] = []

    def _make_estimator(self):
        if self.config.family == "linear":
            return Ridge(alpha=1.0)
        elif self.config.family == "boosting":
            return GradientBoostingRegressor(random_state=self.config.random_state, n_estimators=120, learning_rate=0.05, max_depth=3)
        else:
            raise ValueError(f"Unsupported model family: {self.config.family}")

    def fit(self, train_df: pd.DataFrame) -> "RecursiveForecaster":
        usable = train_df[train_df.get("is_actual", 1) == 1].dropna().copy()
        excluded = {"fecha", "target", "dataset_name", "flag_periodo", *self.config.drop_features}
        self.feature_columns = select_training_features(usable.drop(columns=list(excluded & set(usable.columns)), errors="ignore"))
        x = usable[self.feature_columns].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
        y = pd.to_numeric(usable["target"], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
        self.estimator.fit(x, y)
        return self

    def _build_autoregressive_features(self, history: pd.Series, current_date: pd.Timestamp, row: pd.Series) -> pd.Series:
        row = row.copy()
        delta_factory = (lambda value: pd.Timedelta(days=value)) if self.config.frequency == "daily" else (lambda value: pd.Timedelta(weeks=value))
        for lag in self.config.lags:
            row[f"lag_{lag}"] = history.get(current_date - delta_factory(lag), np.nan)
        shifted = history.sort_index()
        for window in self.config.rolling_windows:
            tail = shifted.tail(window)
            row[f"rolling_mean_{window}"] = tail.mean() if not tail.empty else np.nan
            row[f"rolling_median_{window}"] = tail.median() if not tail.empty else np.nan
        if self.config.frequency == "daily":
            same_dow = shifted[shifted.index.dayofweek == current_date.dayofweek].tail(4)
            row["same_dow_rolling_mean_4"] = same_dow.mean() if not same_dow.empty else np.nan
        return row

    def predict(self, history: pd.Series, future_df: pd.DataFrame) -> pd.DataFrame:
        history = history.copy().sort_index()
        preds = []
        for _, base_row in future_df.sort_values("fecha").iterrows():
            current_date = pd.Timestamp(base_row["fecha"])
            row = self._build_autoregressive_features(history, current_date, base_row)
            x = (
                row[self.feature_columns]
                .to_frame()
                .T.apply(pd.to_numeric, errors="coerce")
                .replace([np.inf, -np.inf], np.nan)
                .fillna(0.0)
            )
            validate_feature_columns(x.columns)
            prediction = float(self.estimator.predict(x)[0])
            prediction = min(max(prediction, 0.0), 1e9)
            history.loc[current_date] = prediction
            preds.append({"fecha": current_date, "prediction": prediction})
        return pd.DataFrame(preds)
