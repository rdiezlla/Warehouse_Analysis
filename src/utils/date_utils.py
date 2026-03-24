from __future__ import annotations

import pandas as pd


def safe_to_datetime(series: pd.Series, *, dayfirst: bool = False) -> tuple[pd.Series, int]:
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=dayfirst)
    failures = int(parsed.isna().sum() - series.isna().sum())
    return parsed, max(failures, 0)


def week_of_month(series: pd.Series) -> pd.Series:
    return ((series.dt.day - 1) // 7 + 1).clip(1, 5)


def add_period_flag(series: pd.Series) -> pd.Series:
    year = pd.to_datetime(series).dt.year
    out = pd.Series("historico_normal", index=series.index)
    out.loc[year == 2025] = "apagado_2025"
    out.loc[year == 2026] = "reactivacion_2026"
    return out


def monday_of_week(series: pd.Series) -> pd.Series:
    series = pd.to_datetime(series)
    return (series - pd.to_timedelta(series.dt.weekday, unit="D")).dt.normalize()
