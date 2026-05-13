from __future__ import annotations

import pandas as pd


def reconcile_service_forecasts(df: pd.DataFrame) -> pd.DataFrame:
    pivot = df.pivot_table(index="fecha", columns="kpi", values="forecast", aggfunc="sum", fill_value=0).reset_index()
    if {"n_entregas_SGE", "n_entregas_SGP"}.issubset(pivot.columns):
        pivot["n_entregas_total"] = pivot["n_entregas_SGE"] + pivot["n_entregas_SGP"]
    return pivot


def reconcile_weekly_from_daily(df: pd.DataFrame) -> pd.DataFrame:
    weekly = df.copy()
    weekly["fecha"] = pd.to_datetime(weekly["fecha"])
    weekly["fecha"] = weekly["fecha"] - pd.to_timedelta(weekly["fecha"].dt.weekday, unit="D")
    return weekly.groupby(["fecha", "kpi"], as_index=False)["forecast"].sum()
