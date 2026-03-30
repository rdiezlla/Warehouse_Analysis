from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from src.modeling.calibration import weekly_horizon_bucket
from src.modeling.forecast_tracking import build_actuals_daily, build_actuals_weekly
from src.paths import CONSUMPTION_DIR, FORECASTS_DIR, REPORTS_DIR
from src.utils.io_utils import ensure_dirs, save_dataframe

MAIN_KPIS = [
    "entregas_sge",
    "entregas_sgp",
    "recogidas_ege",
    "picking_lines_pi",
    "picking_units_pi",
]


@dataclass
class ConsumptionArtifacts:
    consumo_forecast_diario: pd.DataFrame
    consumo_forecast_semanal: pd.DataFrame
    consumo_vs_2024_diario: pd.DataFrame
    consumo_vs_2024_semanal: pd.DataFrame
    consumo_progreso_actual: pd.DataFrame
    dim_kpi: pd.DataFrame
    validations: pd.DataFrame


def _read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def _read_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)


def _latest_run_metadata(daily_forecasts: pd.DataFrame) -> tuple[str, pd.Timestamp, str]:
    if daily_forecasts.empty:
        raise ValueError("daily_forecasts.csv is empty; cannot build consumption layer.")
    latest = daily_forecasts.copy()
    latest["forecast_run_timestamp"] = pd.to_datetime(latest["forecast_run_timestamp"], errors="coerce")
    latest = latest.sort_values(["forecast_run_timestamp", "pipeline_run_id"])
    row = latest.iloc[-1]
    return str(row["pipeline_run_id"]), pd.Timestamp(row["forecast_run_timestamp"]), pd.Timestamp(row["forecast_run_date"]).normalize()


def _coerce_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    output = df.copy()
    for column in columns:
        if column in output.columns:
            output[column] = pd.to_datetime(output[column], errors="coerce").dt.normalize()
    return output


def _safe_pct(numerator: float, denominator: float) -> float:
    if pd.isna(denominator) or denominator == 0:
        return np.nan
    return float(numerator / denominator)


def _safe_divide_diff(current: float, reference: float) -> float:
    if pd.isna(reference) or reference == 0:
        return np.nan
    return float((current - reference) / abs(reference))


def _map_same_calendar_date_to_2024(current_date: pd.Timestamp) -> pd.Timestamp:
    current_date = pd.Timestamp(current_date)
    try:
        return pd.Timestamp(year=2024, month=current_date.month, day=current_date.day)
    except ValueError:
        month_end = pd.Timestamp(year=2024, month=current_date.month, day=1) + pd.offsets.MonthEnd(0)
        return pd.Timestamp(year=2024, month=current_date.month, day=min(current_date.day, month_end.day))


def _monday_for_iso_week(iso_year: int, iso_week: int) -> pd.Timestamp | pd.NaT:
    try:
        return pd.Timestamp(date.fromisocalendar(int(iso_year), int(iso_week), 1))
    except ValueError:
        return pd.NaT


def _load_dim_kpi() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "kpi": "entregas_sge",
                "nombre_negocio": "Entregas SGE",
                "familia_kpi": "servicios",
                "unidad": "servicios",
                "nivel_uso_operativo": "operativo",
                "horizonte_recomendado": "D+1..D+7; semanal oficial tactico",
                "usar_diario": True,
                "usar_semanal": True,
                "comentario_operativo": "Usar en diario corto plazo y semanal oficial agregado.",
                "orden_visual": 1,
            },
            {
                "kpi": "entregas_sgp",
                "nombre_negocio": "Entregas SGP",
                "familia_kpi": "servicios",
                "unidad": "servicios",
                "nivel_uso_operativo": "operativo",
                "horizonte_recomendado": "D+1..D+7; semanal oficial tactico",
                "usar_diario": True,
                "usar_semanal": True,
                "comentario_operativo": "Usar en diario corto plazo; medio plazo mas fragil.",
                "orden_visual": 2,
            },
            {
                "kpi": "recogidas_ege",
                "nombre_negocio": "Recogidas EGE",
                "familia_kpi": "servicios",
                "unidad": "servicios",
                "nivel_uso_operativo": "cautela",
                "horizonte_recomendado": "D+1..D+7 con cautela",
                "usar_diario": True,
                "usar_semanal": True,
                "comentario_operativo": "KPI delicado; revisar siempre actual_truth_status y sesgo.",
                "orden_visual": 3,
            },
            {
                "kpi": "picking_lines_pi",
                "nombre_negocio": "Picking lineas PI",
                "familia_kpi": "picking",
                "unidad": "lineas",
                "nivel_uso_operativo": "operativo",
                "horizonte_recomendado": "D+1..D+14; semanal oficial tactico",
                "usar_diario": True,
                "usar_semanal": True,
                "comentario_operativo": "KPI robusto para operacion diaria y vista semanal.",
                "orden_visual": 4,
            },
            {
                "kpi": "picking_units_pi",
                "nombre_negocio": "Picking unidades PI",
                "familia_kpi": "picking",
                "unidad": "unidades",
                "nivel_uso_operativo": "operativo",
                "horizonte_recomendado": "D+1..D+14; semanal oficial tactico",
                "usar_diario": True,
                "usar_semanal": True,
                "comentario_operativo": "Usar para carga agregada; vigilar sesgo residual.",
                "orden_visual": 5,
            },
        ]
    )


def _build_consumo_forecast_diario(pipeline_run_id: str) -> pd.DataFrame:
    daily_vs_actual = _coerce_dates(
        _read_csv(str(FORECASTS_DIR / "forecast_vs_actual_daily.csv")),
        ["forecast_run_date", "target_date"],
    )
    filtered = daily_vs_actual[
        (daily_vs_actual["pipeline_run_id"] == pipeline_run_id)
        & (daily_vs_actual["kpi"].isin(MAIN_KPIS))
    ].copy()
    filtered = filtered.rename(
        columns={
            "target_date": "fecha",
            "forecast_emitted_at": "forecast_run_timestamp",
        }
    )
    filtered["forecast_run_timestamp"] = pd.to_datetime(filtered["forecast_run_timestamp"], errors="coerce")
    output = filtered[
        [
            "fecha",
            "kpi",
            "forecast_value",
            "actual_value",
            "abs_error",
            "signed_error",
            "ape",
            "forecast_run_date",
            "forecast_run_timestamp",
            "horizon_days",
            "model_name",
            "forecast_source",
            "actual_source",
            "actual_truth_status",
            "service_source_context",
            "pipeline_run_id",
        ]
    ].sort_values(["fecha", "kpi"])
    return output.drop_duplicates(subset=["fecha", "kpi"], keep="last")


def _build_consumo_forecast_semanal(pipeline_run_id: str) -> pd.DataFrame:
    weekly_vs_actual = _coerce_dates(
        _read_csv(str(FORECASTS_DIR / "forecast_vs_actual_weekly.csv")),
        ["forecast_run_date", "target_date"],
    )
    filtered = weekly_vs_actual[
        (weekly_vs_actual["pipeline_run_id"] == pipeline_run_id)
        & (weekly_vs_actual["forecast_source"] == "daily_aggregated")
        & (weekly_vs_actual["kpi"].isin(MAIN_KPIS))
    ].copy()
    filtered = filtered.rename(
        columns={
            "target_date": "week_start_date",
            "forecast_emitted_at": "forecast_run_timestamp",
        }
    )
    filtered["forecast_run_timestamp"] = pd.to_datetime(filtered["forecast_run_timestamp"], errors="coerce")
    filtered["horizon_weeks"] = (filtered["horizon_days"] // 7).astype(int)
    filtered["horizon_weeks_bucket"] = filtered["horizon_weeks"].map(weekly_horizon_bucket)
    output = filtered[
        [
            "week_start_date",
            "kpi",
            "forecast_value",
            "actual_value",
            "abs_error",
            "signed_error",
            "ape",
            "forecast_run_date",
            "forecast_run_timestamp",
            "horizon_weeks",
            "horizon_weeks_bucket",
            "forecast_source",
            "actual_source",
            "actual_truth_status",
            "pipeline_run_id",
        ]
    ].sort_values(["week_start_date", "kpi"])
    return output.drop_duplicates(subset=["week_start_date", "kpi"], keep="last")


def _build_vs_2024_daily(actuals_daily: pd.DataFrame) -> pd.DataFrame:
    current_year = int(pd.to_datetime(actuals_daily["target_date"]).dt.year.max())
    current = actuals_daily[pd.to_datetime(actuals_daily["target_date"]).dt.year == current_year].copy()
    current = current[current["kpi"].isin(MAIN_KPIS)].copy()
    ref_2024 = actuals_daily[pd.to_datetime(actuals_daily["target_date"]).dt.year == 2024].copy()
    ref_2024 = ref_2024[ref_2024["kpi"].isin(MAIN_KPIS)].copy()
    current["comparison_date_2024"] = current["target_date"].map(_map_same_calendar_date_to_2024)
    output = current.merge(
        ref_2024.rename(
            columns={
                "target_date": "comparison_date_2024",
                "actual_value": "actual_value_2024",
                "actual_source": "actual_source_2024",
                "actual_truth_status": "actual_truth_status_2024",
            }
        )[
            ["comparison_date_2024", "kpi", "actual_value_2024", "actual_source_2024", "actual_truth_status_2024"]
        ],
        on=["comparison_date_2024", "kpi"],
        how="left",
    )
    output["diff_abs_vs_2024"] = output["actual_value"] - output["actual_value_2024"]
    output["diff_pct_vs_2024"] = output.apply(lambda row: _safe_divide_diff(row["actual_value"], row["actual_value_2024"]), axis=1)
    output["day_of_week"] = pd.to_datetime(output["target_date"]).dt.dayofweek
    output["week_of_year"] = pd.to_datetime(output["target_date"]).dt.isocalendar().week.astype(int)
    output["month"] = pd.to_datetime(output["target_date"]).dt.month.astype(int)
    output = output.rename(
        columns={
            "target_date": "fecha",
            "actual_value": "actual_value_current",
            "actual_source": "actual_source_current",
            "actual_truth_status": "actual_truth_status_current",
        }
    )
    return output[
        [
            "fecha",
            "kpi",
            "actual_value_current",
            "actual_value_2024",
            "diff_abs_vs_2024",
            "diff_pct_vs_2024",
            "day_of_week",
            "week_of_year",
            "month",
            "actual_source_current",
            "actual_truth_status_current",
            "actual_source_2024",
            "actual_truth_status_2024",
        ]
    ].sort_values(["fecha", "kpi"])


def _build_vs_2024_weekly(actuals_weekly: pd.DataFrame) -> pd.DataFrame:
    weekly = actuals_weekly[actuals_weekly["kpi"].isin(MAIN_KPIS)].copy()
    weekly["iso_week"] = pd.to_datetime(weekly["target_date"]).dt.isocalendar().week.astype(int)
    weekly["iso_year"] = pd.to_datetime(weekly["target_date"]).dt.isocalendar().year.astype(int)
    current_year = int(pd.to_datetime(weekly["target_date"]).dt.year.max())
    current = weekly[pd.to_datetime(weekly["target_date"]).dt.year == current_year].copy()
    ref_2024 = weekly[weekly["iso_year"] == 2024].copy()
    output = current.merge(
        ref_2024.rename(
            columns={
                "actual_value": "actual_value_2024",
                "actual_source": "actual_source_2024",
                "actual_truth_status": "actual_truth_status_2024",
                "target_date": "comparison_week_2024",
            }
        )[
            ["kpi", "iso_week", "actual_value_2024", "actual_source_2024", "actual_truth_status_2024", "comparison_week_2024"]
        ],
        on=["kpi", "iso_week"],
        how="left",
    )
    output["diff_abs_vs_2024"] = output["actual_value"] - output["actual_value_2024"]
    output["diff_pct_vs_2024"] = output.apply(lambda row: _safe_divide_diff(row["actual_value"], row["actual_value_2024"]), axis=1)
    output = output.rename(
        columns={
            "target_date": "week_start_date",
            "actual_value": "actual_value_current",
            "actual_source": "actual_source_current",
            "actual_truth_status": "actual_truth_status_current",
        }
    )
    return output[
        [
            "week_start_date",
            "kpi",
            "actual_value_current",
            "actual_value_2024",
            "diff_abs_vs_2024",
            "diff_pct_vs_2024",
            "iso_week",
            "iso_year",
            "actual_source_current",
            "actual_truth_status_current",
            "actual_source_2024",
            "actual_truth_status_2024",
            "comparison_week_2024",
        ]
    ].sort_values(["week_start_date", "kpi"])


def _latest_pre_target_forecast(history_daily: pd.DataFrame, kpi: str, target_date: pd.Timestamp) -> float:
    subset = history_daily[
        (history_daily["kpi"] == kpi)
        & (history_daily["target_date"] == target_date)
        & (history_daily["forecast_run_date"] < target_date)
    ].copy()
    if subset.empty:
        return np.nan
    subset["forecast_emitted_at"] = pd.to_datetime(subset["forecast_emitted_at"], errors="coerce")
    subset = subset.sort_values(["forecast_run_date", "forecast_emitted_at"])
    return float(subset.iloc[-1]["forecast_value"])


def _period_2024_accumulated(
    actuals_daily: pd.DataFrame,
    kpi: str,
    run_date: pd.Timestamp,
    period_type: str,
) -> tuple[float, str, str]:
    ref = actuals_daily[actuals_daily["kpi"] == kpi].copy()
    if ref.empty:
        return np.nan, "", ""
    if period_type == "day":
        comparison_dates = [_map_same_calendar_date_to_2024(run_date)]
    elif period_type == "week":
        iso_week = int(run_date.isocalendar().week)
        week_start_2024 = _monday_for_iso_week(2024, iso_week)
        day_offset = int(run_date.weekday())
        comparison_dates = list(pd.date_range(week_start_2024, week_start_2024 + pd.Timedelta(days=day_offset), freq="D")) if pd.notna(week_start_2024) else []
    elif period_type == "month":
        month_start_2024 = pd.Timestamp(year=2024, month=run_date.month, day=1)
        month_end_2024 = month_start_2024 + pd.offsets.MonthEnd(0)
        comparison_dates = list(
            pd.date_range(
                month_start_2024,
                pd.Timestamp(year=2024, month=run_date.month, day=min(run_date.day, month_end_2024.day)),
                freq="D",
            )
        )
    else:
        comparison_dates = []
    subset = ref[ref["target_date"].isin(comparison_dates)].copy()
    if subset.empty:
        return np.nan, "", ""
    source = subset["actual_source"].iloc[0] if subset["actual_source"].nunique(dropna=False) == 1 else "mixed"
    truth = subset["actual_truth_status"].iloc[0] if subset["actual_truth_status"].nunique(dropna=False) == 1 else "mixed"
    return float(subset["actual_value"].sum()), str(source), str(truth)


def _build_consumo_progreso_actual(
    run_date: pd.Timestamp,
    current_daily_forecasts: pd.DataFrame,
    history_daily: pd.DataFrame,
    actuals_daily: pd.DataFrame,
) -> pd.DataFrame:
    progress_rows = []
    actuals_focus = actuals_daily[actuals_daily["kpi"].isin(MAIN_KPIS)].copy()
    history_focus = history_daily[history_daily["kpi"].isin(MAIN_KPIS)].copy()
    history_focus = _coerce_dates(history_focus, ["forecast_run_date", "target_date"])

    for kpi in MAIN_KPIS:
        for period_type in ["day", "week", "month"]:
            if period_type == "day":
                period_start = run_date
                period_end = run_date
            elif period_type == "week":
                period_start = run_date - pd.Timedelta(days=run_date.weekday())
                period_end = period_start + pd.Timedelta(days=6)
            else:
                period_start = pd.Timestamp(year=run_date.year, month=run_date.month, day=1)
                period_end = period_start + pd.offsets.MonthEnd(0)

            elapsed_dates = list(pd.date_range(period_start, min(period_end, run_date), freq="D"))
            future_dates = list(pd.date_range(run_date + pd.Timedelta(days=1), period_end, freq="D")) if period_end > run_date else []

            forecast_elapsed_values = [_latest_pre_target_forecast(history_focus, kpi, target_date) for target_date in elapsed_dates]
            valid_elapsed_forecasts = [value for value in forecast_elapsed_values if not pd.isna(value)]
            plan_history_coverage_days = len(valid_elapsed_forecasts)
            plan_history_expected_days = len(elapsed_dates)
            plan_history_coverage_pct = _safe_pct(plan_history_coverage_days, plan_history_expected_days)

            forecast_restante = current_daily_forecasts[
                (current_daily_forecasts["kpi"] == kpi)
                & (current_daily_forecasts["fecha"].isin(future_dates))
            ]["forecast_value"].sum()

            actual_subset = actuals_focus[
                (actuals_focus["kpi"] == kpi)
                & (actuals_focus["target_date"] >= period_start)
                & (actuals_focus["target_date"] <= min(period_end, run_date))
            ].copy()
            actual_acumulado_hasta_hoy = float(actual_subset["actual_value"].sum()) if not actual_subset.empty else np.nan
            actual_source = actual_subset["actual_source"].iloc[0] if not actual_subset.empty and actual_subset["actual_source"].nunique(dropna=False) == 1 else ("mixed" if not actual_subset.empty else "")
            actual_truth_status = actual_subset["actual_truth_status"].iloc[0] if not actual_subset.empty and actual_subset["actual_truth_status"].nunique(dropna=False) == 1 else ("mixed" if not actual_subset.empty else "")

            actual_2024_acumulado, actual_source_2024, actual_truth_status_2024 = _period_2024_accumulated(actuals_focus, kpi, run_date, period_type)
            if plan_history_coverage_days == plan_history_expected_days and plan_history_expected_days > 0:
                forecast_acumulado_hasta_hoy = float(np.nansum(valid_elapsed_forecasts))
                forecast_total_periodo = float(np.nansum([forecast_acumulado_hasta_hoy, forecast_restante]))
                forecast_total_periodo_method = "historical_plan_plus_remaining_forecast"
            else:
                forecast_acumulado_hasta_hoy = np.nan
                forecast_total_periodo = float(np.nansum([actual_acumulado_hasta_hoy, forecast_restante]))
                forecast_total_periodo_method = "actual_plus_remaining_forecast_due_to_partial_plan_history"
            gap_actual_vs_forecast = actual_acumulado_hasta_hoy - forecast_acumulado_hasta_hoy if pd.notna(actual_acumulado_hasta_hoy) and pd.notna(forecast_acumulado_hasta_hoy) else np.nan
            pct_cumplimiento_forecast = _safe_pct(actual_acumulado_hasta_hoy, forecast_acumulado_hasta_hoy)
            diff_vs_2024_acumulado = actual_acumulado_hasta_hoy - actual_2024_acumulado if pd.notna(actual_acumulado_hasta_hoy) and pd.notna(actual_2024_acumulado) else np.nan
            diff_pct_vs_2024_acumulado = _safe_divide_diff(actual_acumulado_hasta_hoy, actual_2024_acumulado)

            progress_rows.append(
                {
                    "fecha_corte": run_date,
                    "tipo_periodo": period_type,
                    "period_start_date": period_start,
                    "period_end_date": period_end,
                    "kpi": kpi,
                    "forecast_total_periodo": forecast_total_periodo,
                    "forecast_acumulado_hasta_hoy": forecast_acumulado_hasta_hoy,
                    "actual_acumulado_hasta_hoy": actual_acumulado_hasta_hoy,
                    "gap_actual_vs_forecast": gap_actual_vs_forecast,
                    "pct_cumplimiento_forecast": pct_cumplimiento_forecast,
                    "plan_history_coverage_days": plan_history_coverage_days,
                    "plan_history_expected_days": plan_history_expected_days,
                    "plan_history_coverage_pct": plan_history_coverage_pct,
                    "forecast_total_periodo_method": forecast_total_periodo_method,
                    "actual_2024_acumulado": actual_2024_acumulado,
                    "diff_vs_2024_acumulado": diff_vs_2024_acumulado,
                    "diff_pct_vs_2024_acumulado": diff_pct_vs_2024_acumulado,
                    "actual_source": actual_source,
                    "actual_truth_status": actual_truth_status,
                    "actual_source_2024": actual_source_2024,
                    "actual_truth_status_2024": actual_truth_status_2024,
                }
            )
    return pd.DataFrame(progress_rows).sort_values(["tipo_periodo", "kpi"])


def _build_validations(
    consumo_forecast_diario: pd.DataFrame,
    consumo_forecast_semanal: pd.DataFrame,
    consumo_vs_2024_diario: pd.DataFrame,
    consumo_vs_2024_semanal: pd.DataFrame,
    consumo_progreso_actual: pd.DataFrame,
    dim_kpi: pd.DataFrame,
) -> pd.DataFrame:
    validations = []
    expected_kpis = set(MAIN_KPIS)
    validations.append(
        {
            "check_name": "daily_kpi_coverage",
            "status": "ok" if set(consumo_forecast_diario["kpi"].unique()) == expected_kpis else "warning",
            "detail": f"KPIs encontrados: {sorted(consumo_forecast_diario['kpi'].unique().tolist())}",
        }
    )
    validations.append(
        {
            "check_name": "weekly_source_official_only",
            "status": "ok" if consumo_forecast_semanal["forecast_source"].eq("daily_aggregated").all() else "error",
            "detail": f"Sources: {sorted(consumo_forecast_semanal['forecast_source'].dropna().unique().tolist())}",
        }
    )
    validations.append(
        {
            "check_name": "vs_2024_daily_coverage",
            "status": "ok" if consumo_vs_2024_diario["actual_value_2024"].notna().mean() >= 0.8 else "warning",
            "detail": f"Cobertura diaria vs 2024: {consumo_vs_2024_diario['actual_value_2024'].notna().mean():.1%}",
        }
    )
    validations.append(
        {
            "check_name": "vs_2024_weekly_coverage",
            "status": "ok" if consumo_vs_2024_semanal["actual_value_2024"].notna().mean() >= 0.8 else "warning",
            "detail": f"Cobertura semanal vs 2024: {consumo_vs_2024_semanal['actual_value_2024'].notna().mean():.1%}",
        }
    )
    validations.append(
        {
            "check_name": "progreso_actual_rows",
            "status": "ok" if len(consumo_progreso_actual) == len(MAIN_KPIS) * 3 else "warning",
            "detail": f"Filas progreso actual: {len(consumo_progreso_actual)}",
        }
    )
    validations.append(
        {
            "check_name": "dim_kpi_operational_policy",
            "status": "ok" if set(dim_kpi["kpi"].unique()) == expected_kpis and dim_kpi.loc[dim_kpi["kpi"] == "recogidas_ege", "nivel_uso_operativo"].iloc[0] == "cautela" else "warning",
            "detail": "recogidas_ege marcado como cautela y dimension completa.",
        }
    )
    validations.append(
        {
            "check_name": "daily_business_key_duplicates",
            "status": "ok" if not consumo_forecast_diario.duplicated(subset=["fecha", "kpi"]).any() else "error",
            "detail": f"Duplicados diario: {int(consumo_forecast_diario.duplicated(subset=['fecha', 'kpi']).sum())}",
        }
    )
    validations.append(
        {
            "check_name": "weekly_business_key_duplicates",
            "status": "ok" if not consumo_forecast_semanal.duplicated(subset=["week_start_date", "kpi"]).any() else "error",
            "detail": f"Duplicados semanal: {int(consumo_forecast_semanal.duplicated(subset=['week_start_date', 'kpi']).sum())}",
        }
    )
    return pd.DataFrame(validations)


def _write_audit(
    validations: pd.DataFrame,
    latest_run_timestamp: pd.Timestamp,
) -> None:
    lines = [
        "# Consumption Layer Audit",
        "",
        "## Tablas generadas",
        "- `consumo_forecast_diario`: consumo diario operativo y seguimiento forecast vs real.",
        "- `consumo_forecast_semanal`: consumo semanal tactico usando solo el semanal oficial derivado del diario.",
        "- `consumo_vs_2024_diario`: comparacion diaria actual vs 2024 por misma fecha calendario.",
        "- `consumo_vs_2024_semanal`: comparacion semanal actual vs 2024 por misma semana ISO.",
        "- `consumo_progreso_actual`: avance del dia, semana y mes actual por KPI.",
        "- `dim_kpi`: dimension de negocio para consumo futuro en dashboard.",
        "",
        "## Fuentes por tabla",
        "- Diario: `daily_forecasts.csv` + `forecast_vs_actual_daily.csv` filtrados al ultimo `pipeline_run_id` operativo.",
        "- Semanal: `weekly_forecasts.csv` + `forecast_vs_actual_weekly.csv`, solo `forecast_source=daily_aggregated`.",
        "- Actuales: `fact_servicio_dia.parquet` y `fact_picking_dia.parquet` via tablas de actuals.",
        "- Progreso actual: `forecast_history_daily.parquet` para la parte planificada acumulada y `daily_forecasts.csv` para el remanente del periodo.",
        "- Cuando no existe historial suficiente para reconstruir el plan acumulado de dias ya transcurridos, `consumo_progreso_actual` deja `forecast_acumulado_hasta_hoy` en `NaN` y estima `forecast_total_periodo` como `actual + forecast restante`.",
        "",
        "## Grano",
        "- Diario: 1 fila por `fecha + kpi`.",
        "- Semanal: 1 fila por `week_start_date + kpi`.",
        "- Vs 2024 diario: 1 fila por `fecha + kpi`.",
        "- Vs 2024 semanal: 1 fila por `week_start_date + kpi`.",
        "- Progreso actual: 1 fila por `tipo_periodo + kpi` para `day/week/month`.",
        "- Progreso actual incluye columnas de cobertura del plan historico para que la web futura sepa si el acumulado forecastado es plenamente comparable.",
        "",
        "## KPIs incluidos",
        "- `entregas_sge`",
        "- `entregas_sgp`",
        "- `recogidas_ege`",
        "- `picking_lines_pi`",
        "- `picking_units_pi`",
        "",
        "## Tabla recomendada por necesidad",
        "- Dashboard diario: `consumo_forecast_diario`.",
        "- Dashboard semanal: `consumo_forecast_semanal`.",
        "- Forecast vs real: `consumo_forecast_diario` y `consumo_forecast_semanal`.",
        "- Comparacion con 2024: `consumo_vs_2024_diario` y `consumo_vs_2024_semanal`.",
        "- Progreso actual del periodo: `consumo_progreso_actual`.",
        "",
        "## actual_truth_status",
        "- Se conserva tal cual en diario y semanal para no mezclar silenciosamente `observado_final` con `visible_cartera`.",
        "- En comparativas con 2024 y progreso actual se conserva tambien el contexto de fuente y truth status cuando aplica.",
        "",
        "## recogidas_ege",
        "- Se mantiene en la capa operativa, pero `dim_kpi` lo marca como `cautela`.",
        "- El dashboard futuro deberia mostrarlo con tratamiento visual distinto.",
        "",
        "## Separacion operativa vs diagnostica",
        "- La capa de consumo semanal usa solo `weekly_forecasts.csv` y `forecast_vs_actual_weekly.csv`.",
        "- `weekly_direct` no entra en esta capa de consumo.",
        "",
        "## Criterio de comparacion con 2024",
        "- Diario: misma fecha calendario en 2024.",
        "- Semanal: misma semana ISO de 2024.",
        "- Esta decision se toma para dar una lectura estable y facil de explicar a negocio.",
        "",
        "## Ultima ejecucion considerada",
        f"- `forecast_run_timestamp`: {latest_run_timestamp.isoformat()}",
        "",
        "## Validaciones",
    ]
    for _, row in validations.iterrows():
        lines.append(f"- `{row['check_name']}`: {row['status']} | {row['detail']}")
    (REPORTS_DIR / "consumption_layer_audit.md").write_text("\n".join(lines), encoding="utf-8")


def build_consumption_layer(context: dict) -> ConsumptionArtifacts:
    ensure_dirs(CONSUMPTION_DIR, REPORTS_DIR)

    daily_forecasts = _coerce_dates(_read_csv(str(FORECASTS_DIR / "daily_forecasts.csv")), ["fecha", "forecast_run_date"])
    daily_forecasts = daily_forecasts[daily_forecasts["kpi"].isin(MAIN_KPIS)].copy()
    weekly_forecasts = _coerce_dates(_read_csv(str(FORECASTS_DIR / "weekly_forecasts.csv")), ["fecha", "forecast_run_date"])
    weekly_forecasts = weekly_forecasts[weekly_forecasts["kpi"].isin(MAIN_KPIS)].copy()
    pipeline_run_id, latest_run_timestamp, run_date = _latest_run_metadata(daily_forecasts)

    actuals_daily = build_actuals_daily(context["fact_servicio_dia"], context["fact_picking_dia"])
    actuals_daily = _coerce_dates(actuals_daily, ["target_date"])
    actuals_weekly = build_actuals_weekly(actuals_daily)
    actuals_weekly = _coerce_dates(actuals_weekly, ["target_date"])
    history_daily = _read_parquet(str(FORECASTS_DIR / "forecast_history_daily.parquet"))

    consumo_forecast_diario = _build_consumo_forecast_diario(pipeline_run_id)
    consumo_forecast_semanal = _build_consumo_forecast_semanal(pipeline_run_id)
    consumo_vs_2024_diario = _build_vs_2024_daily(actuals_daily)
    consumo_vs_2024_semanal = _build_vs_2024_weekly(actuals_weekly)
    dim_kpi = _load_dim_kpi()
    current_daily_for_progress = consumo_forecast_diario.rename(columns={"fecha": "target_date"})[["target_date", "kpi", "forecast_value"]].copy()
    current_daily_for_progress = current_daily_for_progress.rename(columns={"target_date": "fecha"})
    consumo_progreso_actual = _build_consumo_progreso_actual(run_date, current_daily_for_progress, history_daily, actuals_daily)
    validations = _build_validations(
        consumo_forecast_diario,
        consumo_forecast_semanal,
        consumo_vs_2024_diario,
        consumo_vs_2024_semanal,
        consumo_progreso_actual,
        dim_kpi,
    )

    save_dataframe(consumo_forecast_diario, CONSUMPTION_DIR / "consumo_forecast_diario", index=False)
    save_dataframe(consumo_forecast_semanal, CONSUMPTION_DIR / "consumo_forecast_semanal", index=False)
    save_dataframe(consumo_vs_2024_diario, CONSUMPTION_DIR / "consumo_vs_2024_diario", index=False)
    save_dataframe(consumo_vs_2024_semanal, CONSUMPTION_DIR / "consumo_vs_2024_semanal", index=False)
    save_dataframe(consumo_progreso_actual, CONSUMPTION_DIR / "consumo_progreso_actual", index=False)
    save_dataframe(dim_kpi, CONSUMPTION_DIR / "dim_kpi", index=False)
    save_dataframe(validations, CONSUMPTION_DIR / "consumption_layer_validation", index=False)
    _write_audit(validations, latest_run_timestamp)

    return ConsumptionArtifacts(
        consumo_forecast_diario=consumo_forecast_diario,
        consumo_forecast_semanal=consumo_forecast_semanal,
        consumo_vs_2024_diario=consumo_vs_2024_diario,
        consumo_vs_2024_semanal=consumo_vs_2024_semanal,
        consumo_progreso_actual=consumo_progreso_actual,
        dim_kpi=dim_kpi,
        validations=validations,
    )
