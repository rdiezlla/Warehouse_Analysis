from __future__ import annotations

import pandas as pd

from src.features.holiday_features import build_daily_calendar_features, build_weekly_calendar_features
from src.features.lag_features import add_lag_features
from src.utils.date_utils import add_period_flag

SERVICE_TARGETS = {
    "ds_entregas_sge_dia": "n_entregas_SGE_dia",
    "ds_entregas_sgp_dia": "n_entregas_SGP_dia",
    "ds_recogidas_ege_dia": "n_recogidas_EGE_dia",
}


def _assemble_dataset(base: pd.DataFrame, calendar: pd.DataFrame, target_col: str, dataset_name: str, lags: list[int], rolling: list[int], frequency: str) -> pd.DataFrame:
    df = calendar.merge(base[["fecha", target_col]], on="fecha", how="left").fillna({target_col: 0}).sort_values("fecha")
    first_actual_date = pd.to_datetime(base["fecha"]).min()
    last_actual_date = pd.to_datetime(base["fecha"]).max()
    df["target"] = df[target_col].astype(float)
    df = df.drop(columns=[target_col])
    df["dataset_name"] = dataset_name
    df["tipo_servicio"] = dataset_name.split("_")[1].upper()
    df["flag_periodo"] = add_period_flag(df["fecha"])
    df["ramp_up_2026"] = (pd.to_datetime(df["fecha"]) - pd.Timestamp("2026-01-01")).dt.days.clip(lower=0)
    if pd.notna(first_actual_date) and pd.notna(last_actual_date):
        df["is_actual"] = pd.to_datetime(df["fecha"]).between(first_actual_date, last_actual_date).astype(int)
    else:
        df["is_actual"] = 0
    return add_lag_features(df, "target", lags, rolling, frequency=frequency)


def build_service_datasets(fact_servicio_dia: pd.DataFrame, dim_date: pd.DataFrame, settings: dict) -> dict[str, pd.DataFrame]:
    daily_calendar = build_daily_calendar_features(dim_date)
    weekly_calendar = build_weekly_calendar_features(dim_date)
    out: dict[str, pd.DataFrame] = {}
    for dataset_name, target_col in SERVICE_TARGETS.items():
        out[dataset_name] = _assemble_dataset(
            fact_servicio_dia,
            daily_calendar,
            target_col,
            dataset_name,
            settings["features"]["daily_lags"],
            settings["features"]["daily_rolling_windows"],
            "daily",
        )
        weekly_base = fact_servicio_dia[["fecha", target_col]].copy()
        weekly_base["fecha"] = pd.to_datetime(weekly_base["fecha"])
        weekly_base["fecha"] = weekly_base["fecha"] - pd.to_timedelta(weekly_base["fecha"].dt.weekday, unit="D")
        weekly_base = weekly_base.groupby("fecha")[target_col].sum().reset_index()
        out[dataset_name.replace("_dia", "_semana")] = _assemble_dataset(
            weekly_base,
            weekly_calendar,
            target_col,
            dataset_name.replace("_dia", "_semana"),
            settings["features"]["weekly_lags"],
            settings["features"]["weekly_rolling_windows"],
            "weekly",
        )
    return out
