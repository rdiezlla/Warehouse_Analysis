from __future__ import annotations

from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from src.modeling.hierarchical_reconciliation import reconcile_weekly_from_daily
from src.utils.io_utils import read_table, save_parquet_safe

SERVICE_KPI_COLUMN_MAP = {
    "entregas_sge": "n_entregas_SGE_dia",
    "entregas_sgp": "n_entregas_SGP_dia",
    "recogidas_ege": "n_recogidas_EGE_dia",
}

PICKING_KPI_COLUMN_MAP = {
    "picking_lines_pi": "picking_lines_PI_dia",
    "picking_units_pi": "picking_units_PI_dia",
}

FORECAST_TO_ACTUAL_KPI_MAP = {
    "entregas_sge": "entregas_sge",
    "entregas_sgp": "entregas_sgp",
    "recogidas_ege": "recogidas_ege",
    "picking_lines_pi": "picking_lines_pi",
    "picking_units_pi": "picking_units_pi",
    "picking_lines_PI_transformer": "picking_lines_pi",
    "picking_units_PI_transformer": "picking_units_pi",
}


def _service_switch_date_from_fact(fact_servicio_dia: pd.DataFrame) -> pd.Timestamp:
    switch_candidates = pd.to_datetime(fact_servicio_dia.loc[fact_servicio_dia["service_source"] == "solicitudes", "fecha"])
    if switch_candidates.empty:
        return pd.Timestamp.max.normalize()
    return switch_candidates.min().normalize()


def build_service_actual_daily(fact_servicio_dia: pd.DataFrame) -> pd.DataFrame:
    if fact_servicio_dia.empty:
        return pd.DataFrame(columns=["target_date", "kpi", "actual_value", "actual_source", "actual_truth_status"])
    switch_date = _service_switch_date_from_fact(fact_servicio_dia)
    calendar = pd.DataFrame({"target_date": pd.date_range(pd.to_datetime(fact_servicio_dia["fecha"]).min(), pd.to_datetime(fact_servicio_dia["fecha"]).max(), freq="D")})
    merge_columns = ["fecha", "service_source", "service_truth_status", "is_final_service_truth", *SERVICE_KPI_COLUMN_MAP.values()]
    merged = calendar.merge(fact_servicio_dia[merge_columns], left_on="target_date", right_on="fecha", how="left")
    pre_switch = pd.to_datetime(merged["target_date"]) < switch_date
    merged.loc[merged["service_source"].isna() & pre_switch, "service_source"] = "albaranes"
    merged.loc[merged["service_source"].isna() & ~pre_switch, "service_source"] = "solicitudes"
    merged.loc[merged["service_truth_status"].isna() & pre_switch, "service_truth_status"] = "observado_final"
    merged.loc[merged["service_truth_status"].isna() & ~pre_switch, "service_truth_status"] = "visible_cartera"
    merged["is_final_service_truth"] = merged["is_final_service_truth"].fillna(pre_switch.astype(int)).astype(int)
    for column in SERVICE_KPI_COLUMN_MAP.values():
        merged[column] = merged[column].fillna(0.0)
    actuals = merged.melt(
        id_vars=["target_date", "service_source", "service_truth_status", "is_final_service_truth"],
        value_vars=list(SERVICE_KPI_COLUMN_MAP.values()),
        var_name="metric",
        value_name="actual_value",
    )
    inverse_map = {value: key for key, value in SERVICE_KPI_COLUMN_MAP.items()}
    actuals["kpi"] = actuals["metric"].map(inverse_map)
    actuals["actual_source"] = actuals["service_source"]
    actuals["actual_truth_status"] = actuals["service_truth_status"]
    return actuals[["target_date", "kpi", "actual_value", "actual_source", "actual_truth_status"]]


def build_picking_actual_daily(fact_picking_dia: pd.DataFrame) -> pd.DataFrame:
    if fact_picking_dia.empty:
        return pd.DataFrame(columns=["target_date", "kpi", "actual_value", "actual_source", "actual_truth_status"])
    calendar = pd.DataFrame({"target_date": pd.date_range(pd.to_datetime(fact_picking_dia["fecha"]).min(), pd.to_datetime(fact_picking_dia["fecha"]).max(), freq="D")})
    merged = calendar.merge(fact_picking_dia[["fecha", *PICKING_KPI_COLUMN_MAP.values()]], left_on="target_date", right_on="fecha", how="left")
    for column in PICKING_KPI_COLUMN_MAP.values():
        merged[column] = merged[column].fillna(0.0)
    actuals = merged.melt(
        id_vars=["target_date"],
        value_vars=list(PICKING_KPI_COLUMN_MAP.values()),
        var_name="metric",
        value_name="actual_value",
    )
    inverse_map = {value: key for key, value in PICKING_KPI_COLUMN_MAP.items()}
    actuals["kpi"] = actuals["metric"].map(inverse_map)
    actuals["actual_source"] = "movimientos"
    actuals["actual_truth_status"] = "observado_final"
    return actuals[["target_date", "kpi", "actual_value", "actual_source", "actual_truth_status"]]


def build_actuals_daily(fact_servicio_dia: pd.DataFrame, fact_picking_dia: pd.DataFrame) -> pd.DataFrame:
    return pd.concat(
        [
            build_service_actual_daily(fact_servicio_dia),
            build_picking_actual_daily(fact_picking_dia),
        ],
        ignore_index=True,
    )


def build_actuals_weekly(actuals_daily: pd.DataFrame) -> pd.DataFrame:
    if actuals_daily.empty:
        return pd.DataFrame(columns=["target_date", "kpi", "actual_value", "actual_source", "actual_truth_status"])
    weekly = actuals_daily.copy()
    weekly["target_date"] = pd.to_datetime(weekly["target_date"])
    weekly["target_date"] = weekly["target_date"] - pd.to_timedelta(weekly["target_date"].dt.weekday, unit="D")
    out = (
        weekly.groupby(["target_date", "kpi"], as_index=False)
        .agg(
            actual_value=("actual_value", "sum"),
            actual_source=("actual_source", lambda x: x.iloc[0] if x.nunique(dropna=False) == 1 else "mixed"),
            actual_truth_status=("actual_truth_status", lambda x: x.iloc[0] if x.nunique(dropna=False) == 1 else "mixed"),
        )
    )
    return out


def prepare_forecast_output_for_history(
    forecast_df: pd.DataFrame,
    run_timestamp: pd.Timestamp,
    pipeline_run_id: str,
    frequency: str,
    switch_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    if forecast_df.empty:
        return pd.DataFrame(
            columns=[
                "forecast_emitted_at",
                "forecast_run_date",
                "target_date",
                "kpi",
                "forecast_value",
                "model_name",
                "forecast_source",
                "service_source_context",
                "forecast_version",
                "pipeline_run_id",
                "frequency",
            ]
        )
    run_timestamp = pd.Timestamp(run_timestamp)
    if getattr(run_timestamp, "tzinfo", None) is not None:
        run_date = run_timestamp.tz_localize(None).normalize()
    else:
        run_date = run_timestamp.normalize()
    history = forecast_df.copy()
    history["forecast_emitted_at"] = run_timestamp.isoformat()
    history["forecast_run_date"] = run_date.date().isoformat()
    history["target_date"] = pd.to_datetime(history["fecha"]).dt.normalize()
    history["forecast_value"] = pd.to_numeric(history["forecast"], errors="coerce")
    history["forecast_source"] = history.get("source", "current_operational_output")
    history["service_source_context"] = "movimientos"
    if switch_date is not None:
        service_mask = history["kpi"].isin(["entregas_sge", "entregas_sgp", "recogidas_ege"])
        history.loc[service_mask & (history["target_date"] < switch_date), "service_source_context"] = "albaranes"
        history.loc[service_mask & (history["target_date"] >= switch_date), "service_source_context"] = "solicitudes"
    history["forecast_version"] = "service_layer_hybrid_v1"
    history["pipeline_run_id"] = pipeline_run_id
    history["frequency"] = frequency
    if "model_name" not in history.columns:
        history["model_name"] = np.nan
    return history[
        [
            "forecast_emitted_at",
            "forecast_run_date",
            "target_date",
            "kpi",
            "forecast_value",
            "model_name",
            "forecast_source",
            "service_source_context",
            "forecast_version",
            "pipeline_run_id",
            "frequency",
        ]
    ]


def append_forecast_history(history_rows: pd.DataFrame, history_path: Path) -> pd.DataFrame:
    if history_rows.empty:
        existing = read_table(history_path) if history_path.exists() else history_rows
        return existing
    existing = pd.DataFrame(columns=history_rows.columns)
    if history_path.exists():
        existing = read_table(history_path)
        missing_columns = [column for column in history_rows.columns if column not in existing.columns]
        if missing_columns:
            obsolete_path = history_path.with_name(f"{history_path.stem}_obsolete{history_path.suffix}")
            shutil.move(history_path, obsolete_path)
            existing = pd.DataFrame(columns=history_rows.columns)
    for frame in [existing, history_rows]:
        if not frame.empty:
            frame["forecast_run_date"] = pd.to_datetime(frame["forecast_run_date"], errors="coerce").dt.normalize()
            frame["target_date"] = pd.to_datetime(frame["target_date"], errors="coerce").dt.normalize()
    combined = history_rows.copy() if existing.empty else pd.concat([existing, history_rows], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["pipeline_run_id", "target_date", "kpi", "model_name", "forecast_source", "frequency"],
        keep="last",
    ).sort_values(["forecast_run_date", "target_date", "kpi", "model_name"], na_position="last")
    save_parquet_safe(combined, history_path, index=False)
    return combined


def build_forecast_vs_actual(history_df: pd.DataFrame, actuals_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return pd.DataFrame(
            columns=[
                "forecast_run_date",
                "forecast_emitted_at",
                "target_date",
                "kpi",
                "forecast_value",
                "actual_value",
                "abs_error",
                "signed_error",
                "ape",
                "horizon_days",
                "model_name",
                "actual_source",
                "actual_truth_status",
            ]
        )
    actual_lookup = actuals_df.rename(columns={"kpi": "actual_kpi"})
    out = history_df.copy()
    out["actual_kpi"] = out["kpi"].map(FORECAST_TO_ACTUAL_KPI_MAP)
    out = out.merge(actual_lookup, left_on=["target_date", "actual_kpi"], right_on=["target_date", "actual_kpi"], how="left")
    out["signed_error"] = out["forecast_value"] - out["actual_value"]
    out["abs_error"] = out["signed_error"].abs()
    out["ape"] = np.where(pd.to_numeric(out["actual_value"], errors="coerce").abs() > 0, out["abs_error"] / out["actual_value"].abs(), np.nan)
    out["horizon_days"] = (pd.to_datetime(out["target_date"]) - pd.to_datetime(out["forecast_run_date"])).dt.days
    return out[
        [
            "forecast_run_date",
            "forecast_emitted_at",
            "target_date",
            "kpi",
            "forecast_value",
            "actual_value",
            "abs_error",
            "signed_error",
            "ape",
            "horizon_days",
            "model_name",
            "actual_source",
            "actual_truth_status",
            "forecast_source",
            "service_source_context",
            "forecast_version",
            "pipeline_run_id",
            "frequency",
        ]
    ]


def build_weekly_history_from_daily(daily_history_rows: pd.DataFrame) -> pd.DataFrame:
    if daily_history_rows.empty:
        return daily_history_rows.copy()
    weekly_frames = []
    for (forecast_emitted_at, forecast_run_date, pipeline_run_id), group in daily_history_rows.groupby(
        ["forecast_emitted_at", "forecast_run_date", "pipeline_run_id"],
        dropna=False,
    ):
        weekly = group.copy()
        weekly["fecha"] = pd.to_datetime(weekly["target_date"])
        weekly["forecast"] = weekly["forecast_value"]
        weekly = reconcile_weekly_from_daily(weekly[["fecha", "kpi", "forecast"]]).rename(columns={"fecha": "target_date", "forecast": "forecast_value"})
        weekly["forecast_emitted_at"] = forecast_emitted_at
        weekly["forecast_run_date"] = forecast_run_date
        weekly["pipeline_run_id"] = pipeline_run_id
        weekly["frequency"] = "weekly"
        weekly["forecast_source"] = "daily_aggregated"
        weekly["service_source_context"] = weekly["kpi"].map(lambda value: "movimientos" if value.startswith("picking") else "hybrid_service_layer")
        weekly["forecast_version"] = "service_layer_hybrid_v1"
        weekly["model_name"] = np.nan
        weekly_frames.append(weekly)
    return pd.concat(weekly_frames, ignore_index=True)[
        [
            "forecast_emitted_at",
            "forecast_run_date",
            "target_date",
            "kpi",
            "forecast_value",
            "model_name",
            "forecast_source",
            "service_source_context",
            "forecast_version",
            "pipeline_run_id",
            "frequency",
        ]
    ]
