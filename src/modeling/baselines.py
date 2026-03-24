from __future__ import annotations

import numpy as np
import pandas as pd


def seasonal_naive(history: pd.Series, future_dates: pd.DatetimeIndex, seasonal_lag: int) -> pd.Series:
    values = history.copy()
    preds = []
    fallback = float(history.tail(min(len(history), seasonal_lag)).mean()) if not history.empty else 0.0
    inferred_freq = history.index.inferred_freq or "D"
    delta = pd.Timedelta(weeks=seasonal_lag) if inferred_freq.startswith("W") else pd.Timedelta(days=seasonal_lag)
    for future_date in future_dates:
        reference_date = future_date - delta
        value = values.get(reference_date, np.nan)
        if pd.isna(value):
            value = fallback
        preds.append(max(float(value), 0.0))
        values.loc[future_date] = value
    return pd.Series(preds, index=future_dates)


def moving_average(history: pd.Series, future_dates: pd.DatetimeIndex, window: int) -> pd.Series:
    values = history.copy()
    preds = []
    for future_date in future_dates:
        value = float(values.tail(window).mean()) if not values.empty else 0.0
        preds.append(max(value, 0.0))
        values.loc[future_date] = value
    return pd.Series(preds, index=future_dates)


def median_by_day_of_week(history: pd.Series, future_dates: pd.DatetimeIndex, fallback_window: int = 84) -> pd.Series:
    values = history.copy()
    preds = []
    for future_date in future_dates:
        reference = values.tail(fallback_window)
        same_dow = reference[reference.index.dayofweek == future_date.dayofweek]
        value = float(same_dow.median()) if not same_dow.empty else float(reference.median() if not reference.empty else 0.0)
        preds.append(max(value, 0.0))
        values.loc[future_date] = value
    return pd.Series(preds, index=future_dates)
