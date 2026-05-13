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

SERVICE_DATASET_TO_TIPO = {
    "ds_entregas_sge_dia": "SGE",
    "ds_entregas_sgp_dia": "SGP",
    "ds_recogidas_ege_dia": "EGE",
    "ds_entregas_sge_semana": "SGE",
    "ds_entregas_sgp_semana": "SGP",
    "ds_recogidas_ege_semana": "EGE",
}


def resolve_service_type(dataset_name: str) -> str:
    service_type = SERVICE_DATASET_TO_TIPO.get(dataset_name)
    if service_type is None:
        raise ValueError(f"Unexpected service dataset_name without explicit tipo_servicio mapping: {dataset_name}")
    return service_type


def _assemble_dataset(base: pd.DataFrame, calendar: pd.DataFrame, target_col: str, dataset_name: str, lags: list[int], rolling: list[int], frequency: str) -> pd.DataFrame:
    switch_date = pd.Timestamp("1900-01-01")
    if "service_source" in base.columns:
        switch_candidates = pd.to_datetime(base.loc[base["service_source"] == "solicitudes", "fecha"])
        if not switch_candidates.empty:
            switch_date = switch_candidates.min().normalize()
    merge_columns = ["fecha", target_col]
    for column in ["service_source", "service_truth_status", "is_final_service_truth"]:
        if column in base.columns:
            merge_columns.append(column)
    df = calendar.merge(base[merge_columns], on="fecha", how="left").fillna({target_col: 0}).sort_values("fecha")
    first_actual_date = pd.to_datetime(base["fecha"]).min()
    last_actual_date = pd.to_datetime(base["fecha"]).max()
    df["target"] = df[target_col].astype(float)
    df = df.drop(columns=[target_col])
    df["dataset_name"] = dataset_name
    df["tipo_servicio"] = resolve_service_type(dataset_name)
    df["service_source"] = df.get("service_source", pd.Series(index=df.index, dtype=object))
    df["service_truth_status"] = df.get("service_truth_status", pd.Series(index=df.index, dtype=object))
    df["is_final_service_truth"] = df.get("is_final_service_truth", pd.Series(index=df.index, dtype=float))
    pre_switch_mask = pd.to_datetime(df["fecha"]) < switch_date
    df.loc[df["service_source"].isna() & pre_switch_mask, "service_source"] = "albaranes"
    df.loc[df["service_source"].isna() & ~pre_switch_mask, "service_source"] = "solicitudes"
    df.loc[df["service_truth_status"].isna() & pre_switch_mask, "service_truth_status"] = "observado_final"
    df.loc[df["service_truth_status"].isna() & ~pre_switch_mask, "service_truth_status"] = "visible_cartera"
    df.loc[df["is_final_service_truth"].isna() & pre_switch_mask, "is_final_service_truth"] = 1
    df.loc[df["is_final_service_truth"].isna() & ~pre_switch_mask, "is_final_service_truth"] = 0
    df["is_final_service_truth"] = df["is_final_service_truth"].astype(int)
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
