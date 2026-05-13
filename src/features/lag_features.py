from __future__ import annotations

import numpy as np
import pandas as pd


def add_lag_features(df: pd.DataFrame, target_col: str, lags: list[int], rolling_windows: list[int], frequency: str = "daily") -> pd.DataFrame:
    out = df.copy()
    for lag in lags:
        out[f"lag_{lag}"] = out[target_col].shift(lag)
    for window in rolling_windows:
        shifted = out[target_col].shift(1)
        out[f"rolling_mean_{window}"] = shifted.rolling(window, min_periods=1).mean()
        out[f"rolling_median_{window}"] = shifted.rolling(window, min_periods=1).median()
    if frequency == "daily":
        same_dow_mean = []
        for idx, row in out.iterrows():
            history = out.iloc[:idx] if isinstance(idx, int) else out.iloc[:0]
            mask = history["day_of_week"] == row["day_of_week"] if not history.empty else pd.Series(dtype=bool)
            values = history.loc[mask, target_col].tail(4)
            same_dow_mean.append(values.mean() if not values.empty else np.nan)
        out["same_dow_rolling_mean_4"] = same_dow_mean
    return out
