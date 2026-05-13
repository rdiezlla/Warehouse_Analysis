from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.data.service_date_utils import get_service_target_date_column
from src.main import _best_model_name, _current_run_timestamp, _predict_with_model_name, ensure_processed_context, load_config, load_datasets
from src.modeling.forecast_tracking import build_actuals_daily, build_actuals_weekly
from src.modeling.nowcasting import apply_nowcasting, fit_cartera_scalers
from src.paths import BACKTESTS_DIR, FORECASTS_DIR, PLOTS_DIR, PROCESSED_DIR, REPORTS_DIR
from src.utils.io_utils import ensure_dirs

LOGGER = logging.getLogger(__name__)

SERVICE_DATASETS = {
    "ds_entregas_sge_dia": "entregas_sge",
    "ds_entregas_sgp_dia": "entregas_sgp",
    "ds_recogidas_ege_dia": "recogidas_ege",
}

PICKING_DATASETS = {
    "ds_picking_lines_pi_dia": "picking_lines_pi",
    "ds_picking_units_pi_dia": "picking_units_pi",
}

MAIN_DATASETS = {**SERVICE_DATASETS, **PICKING_DATASETS}
RECENT_START = pd.Timestamp("2026-03-01")


def _daily_bucket(horizon_days: int) -> str | None:
    if horizon_days == 1:
        return "D+1"
    if 2 <= horizon_days <= 3:
        return "D+2_a_D+3"
    if 4 <= horizon_days <= 7:
        return "D+4_a_D+7"
    if 8 <= horizon_days <= 14:
        return "D+8_a_D+14"
    if 15 <= horizon_days <= 28:
        return "D+15_a_D+28"
    return None


def _weekly_bucket(horizon_weeks: int) -> str | None:
    if horizon_weeks == 1:
        return "W+1"
    if horizon_weeks == 2:
        return "W+2"
    if 3 <= horizon_weeks <= 4:
        return "W+3_a_W+4"
    if horizon_weeks >= 5:
        return "W+5+"
    return None


def _safe_wape(actual: pd.Series, abs_error: pd.Series) -> float:
    denom = actual.abs().sum()
    if denom == 0 or pd.isna(denom):
        return np.nan
    return float(abs_error.sum() / denom)


def _safe_bias(error: pd.Series) -> float:
    if error.empty:
        return np.nan
    return float(error.mean())


def _summarize(df: pd.DataFrame, bucket_col: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "kpi",
                bucket_col,
                "actual_truth_status",
                "actual_source",
                "n_obs",
                "mae",
                "wape",
                "bias",
                "median_abs_error",
                "p90_abs_error",
            ]
        )
    rows = []
    group_cols = ["kpi", bucket_col, "actual_truth_status", "actual_source"]
    if "forecast_source" in df.columns:
        group_cols.append("forecast_source")
    for keys, group in df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        row["n_obs"] = int(group["actual_value"].notna().sum())
        if row["n_obs"] == 0:
            continue
        valid = group.dropna(subset=["actual_value", "abs_error", "signed_error"]).copy()
        row["mae"] = float(valid["abs_error"].mean()) if not valid.empty else np.nan
        row["wape"] = _safe_wape(valid["actual_value"], valid["abs_error"]) if not valid.empty else np.nan
        row["bias"] = _safe_bias(valid["signed_error"]) if not valid.empty else np.nan
        row["median_abs_error"] = float(valid["abs_error"].median()) if not valid.empty else np.nan
        row["p90_abs_error"] = float(valid["abs_error"].quantile(0.90)) if not valid.empty else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def _horizon_weeks(run_date: pd.Timestamp, target_date: pd.Timestamp) -> int:
    run_week = run_date - pd.Timedelta(days=run_date.weekday())
    target_week = target_date - pd.Timedelta(days=target_date.weekday())
    return int((target_week - run_week).days // 7)


def _service_switch_date(settings: dict, context: dict) -> pd.Timestamp:
    configured = settings.get("service_layer", {}).get("switch_date")
    if configured:
        return pd.Timestamp(configured).normalize()
    switch_date = context.get("service_layer_meta", {}).get("switch_date")
    return pd.Timestamp(switch_date).normalize() if pd.notna(switch_date) else RECENT_START


def _replay_recent_daily(settings: dict, datasets: dict[str, pd.DataFrame], context: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    run_date_end = _current_run_timestamp(settings).tz_localize(None).normalize()
    actuals_daily = build_actuals_daily(context["fact_servicio_dia"], context["fact_picking_dia"])
    metrics = pd.read_csv(BACKTESTS_DIR / "backtest_metrics_all.csv") if (BACKTESTS_DIR / "backtest_metrics_all.csv").exists() else pd.DataFrame()
    switch_date = _service_switch_date(settings, context)
    final_rows: list[pd.DataFrame] = []
    nowcast_rows: list[pd.DataFrame] = []
    scalers = fit_cartera_scalers(context["fact_servicio_dia"], context["fact_cartera"])

    for dataset_name, kpi in MAIN_DATASETS.items():
        df = datasets[dataset_name].copy()
        model_name = _best_model_name(metrics, dataset_name)
        service_type = str(df["tipo_servicio"].iloc[0])
        for run_date in pd.date_range(RECENT_START, run_date_end, freq="D"):
            observed = df[(df["is_actual"] == 1) & (pd.to_datetime(df["fecha"]) <= run_date)].copy()
            if observed.empty:
                continue
            future = df[pd.to_datetime(df["fecha"]) > run_date].head(settings["training"]["daily_horizon"]).copy()
            if future.empty:
                continue
            history = observed.set_index(pd.to_datetime(observed["fecha"]))["target"].astype(float)
            base_forecast = _predict_with_model_name(df, settings, "daily", model_name, history, future).rename(columns={"prediction": "forecast_base"})
            base_forecast["target_date"] = pd.to_datetime(base_forecast["fecha"]).dt.normalize()
            base_forecast["forecast_run_date"] = run_date
            base_forecast["kpi"] = kpi
            base_forecast["model_name"] = model_name
            base_forecast["forecast_source"] = "service_model" if dataset_name in SERVICE_DATASETS else "picking_model"
            base_forecast["horizon_days"] = (base_forecast["target_date"] - run_date).dt.days

            if dataset_name in SERVICE_DATASETS:
                nowcast_input = base_forecast.rename(columns={"forecast_base": "forecast"}).copy()
                nowcast_input["tipo_servicio"] = service_type
                adjusted = apply_nowcasting(
                    nowcast_input[["fecha", "forecast", "tipo_servicio"]],
                    context["fact_cartera"],
                    scalers,
                    run_date,
                    settings,
                    maturity_curves=context["cartera_maturity_curves"],
                ).rename(columns={"forecast": "forecast_final"})
                merged = base_forecast.merge(adjusted[["fecha", "forecast_final"]], on="fecha", how="left")
                merged["forecast_final"] = merged["forecast_final"].fillna(merged["forecast_base"])
                merged["target_date"] = pd.to_datetime(merged["fecha"]).dt.normalize()
                merged["service_source_context"] = np.where(merged["target_date"] >= switch_date, "solicitudes", "albaranes")
                nowcast_rows.append(merged)
                final = merged.rename(columns={"forecast_final": "forecast_value"})[
                    ["forecast_run_date", "target_date", "kpi", "forecast_value", "model_name", "forecast_source", "service_source_context", "horizon_days"]
                ]
            else:
                final = base_forecast.rename(columns={"forecast_base": "forecast_value"})[
                    ["forecast_run_date", "target_date", "kpi", "forecast_value", "model_name", "forecast_source", "horizon_days"]
                ]
                final["service_source_context"] = "movimientos"
            final_rows.append(final)

    final_df = pd.concat(final_rows, ignore_index=True) if final_rows else pd.DataFrame()
    if not final_df.empty:
        final_df = final_df.merge(actuals_daily, on=["target_date", "kpi"], how="left")
        final_df["signed_error"] = final_df["forecast_value"] - final_df["actual_value"]
        final_df["abs_error"] = final_df["signed_error"].abs()
        final_df["horizon_bucket"] = final_df["horizon_days"].map(_daily_bucket)
        final_df = final_df[(final_df["target_date"] >= RECENT_START) & final_df["horizon_bucket"].notna()].copy()

    nowcast_df = pd.concat(nowcast_rows, ignore_index=True) if nowcast_rows else pd.DataFrame()
    if not nowcast_df.empty:
        nowcast_df = nowcast_df.merge(actuals_daily, on=["target_date", "kpi"], how="left")
        nowcast_df["base_signed_error"] = nowcast_df["forecast_base"] - nowcast_df["actual_value"]
        nowcast_df["base_abs_error"] = nowcast_df["base_signed_error"].abs()
        nowcast_df["final_signed_error"] = nowcast_df["forecast_final"] - nowcast_df["actual_value"]
        nowcast_df["final_abs_error"] = nowcast_df["final_signed_error"].abs()
        nowcast_df["horizon_bucket"] = nowcast_df["horizon_days"].map(_daily_bucket)
        nowcast_df = nowcast_df[(nowcast_df["target_date"] >= RECENT_START) & nowcast_df["horizon_bucket"].notna()].copy()
    return final_df, nowcast_df


def _replay_recent_weekly(settings: dict, datasets: dict[str, pd.DataFrame], context: dict) -> pd.DataFrame:
    run_date_end = _current_run_timestamp(settings).tz_localize(None).normalize()
    actuals_weekly = build_actuals_weekly(build_actuals_daily(context["fact_servicio_dia"], context["fact_picking_dia"]))
    metrics = pd.read_csv(BACKTESTS_DIR / "backtest_metrics_all.csv") if (BACKTESTS_DIR / "backtest_metrics_all.csv").exists() else pd.DataFrame()
    rows: list[pd.DataFrame] = []

    weekly_targets = {
        "ds_entregas_sge_semana": "entregas_sge",
        "ds_entregas_sgp_semana": "entregas_sgp",
        "ds_recogidas_ege_semana": "recogidas_ege",
        "ds_picking_lines_pi_semana": "picking_lines_pi",
        "ds_picking_units_pi_semana": "picking_units_pi",
    }

    for dataset_name, kpi in weekly_targets.items():
        df = datasets[dataset_name].copy()
        model_name = _best_model_name(metrics, dataset_name)
        for run_date in pd.date_range(RECENT_START, run_date_end, freq="D"):
            observed = df[(df["is_actual"] == 1) & (pd.to_datetime(df["fecha"]) <= run_date)].copy()
            if observed.empty:
                continue
            future = df[pd.to_datetime(df["fecha"]) > run_date].head(settings["training"]["weekly_horizon"]).copy()
            if future.empty:
                continue
            history = observed.set_index(pd.to_datetime(observed["fecha"]))["target"].astype(float)
            forecast = _predict_with_model_name(df, settings, "weekly", model_name, history, future).rename(columns={"prediction": "forecast_value"})
            forecast["target_date"] = pd.to_datetime(forecast["fecha"]).dt.normalize()
            forecast["forecast_run_date"] = run_date
            forecast["kpi"] = kpi
            forecast["model_name"] = model_name
            forecast["forecast_source"] = "weekly_direct"
            forecast["horizon_weeks"] = forecast["target_date"].map(lambda date: _horizon_weeks(run_date, pd.Timestamp(date)))
            rows.append(forecast[["forecast_run_date", "target_date", "kpi", "forecast_value", "model_name", "forecast_source", "horizon_weeks"]])

    weekly_df = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if not weekly_df.empty:
        weekly_df = weekly_df.merge(actuals_weekly, on=["target_date", "kpi"], how="left")
        weekly_df["signed_error"] = weekly_df["forecast_value"] - weekly_df["actual_value"]
        weekly_df["abs_error"] = weekly_df["signed_error"].abs()
        weekly_df["horizon_bucket"] = weekly_df["horizon_weeks"].map(_weekly_bucket)
        weekly_df = weekly_df[(weekly_df["target_date"] >= RECENT_START) & weekly_df["horizon_bucket"].notna()].copy()
    return weekly_df


def _build_nowcasting_impact(nowcast_df: pd.DataFrame) -> pd.DataFrame:
    if nowcast_df.empty:
        return pd.DataFrame()
    rows = []
    for (kpi, bucket), group in nowcast_df.groupby(["kpi", "horizon_bucket"], dropna=False):
        valid = group.dropna(subset=["actual_value"]).copy()
        if valid.empty:
            continue
        base_mae = float(valid["base_abs_error"].mean())
        final_mae = float(valid["final_abs_error"].mean())
        base_wape = _safe_wape(valid["actual_value"], valid["base_abs_error"])
        final_wape = _safe_wape(valid["actual_value"], valid["final_abs_error"])
        base_bias = _safe_bias(valid["base_signed_error"])
        final_bias = _safe_bias(valid["final_signed_error"])
        label = "neutral"
        if np.isfinite(base_mae) and np.isfinite(final_mae):
            if final_mae < base_mae * 0.95 or (np.isfinite(base_wape) and np.isfinite(final_wape) and final_wape < base_wape * 0.95):
                label = "helpful"
            elif final_mae > base_mae * 1.05 or (np.isfinite(base_wape) and np.isfinite(final_wape) and final_wape > base_wape * 1.05):
                label = "harmful"
        rows.append(
            {
                "kpi": kpi,
                "horizon_bucket": bucket,
                "n_obs": int(valid["actual_value"].notna().sum()),
                "base_mae": base_mae,
                "final_mae": final_mae,
                "delta_mae": final_mae - base_mae,
                "base_wape": base_wape,
                "final_wape": final_wape,
                "delta_wape": final_wape - base_wape if np.isfinite(base_wape) and np.isfinite(final_wape) else np.nan,
                "base_bias": base_bias,
                "final_bias": final_bias,
                "label": label,
            }
        )
    return pd.DataFrame(rows)


def _audit_service_logic() -> tuple[dict[str, float], pd.DataFrame]:
    solicitudes = pd.read_parquet(PROCESSED_DIR / "solicitudes_maestro.parquet")
    target_col = get_service_target_date_column(solicitudes, "fecha_inicio_evento")
    df = solicitudes.dropna(subset=["codigo_generico", target_col]).copy()
    df[target_col] = pd.to_datetime(df[target_col]).dt.normalize()
    version_candidates = []
    for column in ["ultima_modificacion", "modificacion_linea", "fecha_creacion", "creacion_solicitud"]:
        if column in df.columns:
            version_candidates.append(pd.to_datetime(df[column], errors="coerce"))
    df["version_ts"] = pd.concat(version_candidates, axis=1).max(axis=1) if version_candidates else pd.NaT
    df["version_ts"] = df["version_ts"].fillna(pd.Timestamp("1900-01-01"))
    summary = (
        df.groupby("codigo_generico")
        .agg(
            min_fecha=(target_col, "min"),
            max_fecha=(target_col, "max"),
            n_fechas=(target_col, "nunique"),
            unidades=("cant_solicitada", "sum"),
        )
        .reset_index()
    )
    latest_rows = (
        df.sort_values(["codigo_generico", "version_ts", target_col])
        .groupby("codigo_generico", as_index=False)
        .tail(1)[["codigo_generico", target_col]]
        .rename(columns={target_col: "latest_known_fecha"})
    )
    summary = summary.merge(latest_rows, on="codigo_generico", how="left")
    summary["shift_days_min_to_latest"] = (summary["latest_known_fecha"] - summary["min_fecha"]).dt.days.fillna(0)
    multi = summary[summary["n_fechas"] > 1].copy()
    metrics = {
        "n_codigos": int(summary["codigo_generico"].nunique()),
        "pct_codigos_multi_fecha": float((summary["n_fechas"] > 1).mean()),
        "pct_multi_fecha_min_diff_latest": float((multi["min_fecha"] != multi["latest_known_fecha"]).mean()) if not multi.empty else 0.0,
        "median_shift_days_min_to_latest": float(multi["shift_days_min_to_latest"].median()) if not multi.empty else 0.0,
        "p90_shift_days_min_to_latest": float(multi["shift_days_min_to_latest"].quantile(0.90)) if not multi.empty else 0.0,
        "max_shift_days_min_to_latest": float(multi["shift_days_min_to_latest"].max()) if not multi.empty else 0.0,
    }
    examples = multi.sort_values("shift_days_min_to_latest", ascending=False).head(20)
    return metrics, examples


def _plot_daily_metric(validation_daily: pd.DataFrame, value_col: str, output_name: str) -> None:
    if validation_daily.empty:
        return
    ordered_buckets = ["D+1", "D+2_a_D+3", "D+4_a_D+7", "D+8_a_D+14", "D+15_a_D+28"]
    fig, axes = plt.subplots(3, 2, figsize=(14, 12), sharex=True)
    axes = axes.flatten()
    for ax, kpi in zip(axes, sorted(validation_daily["kpi"].unique())):
        subset = validation_daily[validation_daily["kpi"] == kpi].copy()
        subset["horizon_bucket"] = pd.Categorical(subset["horizon_bucket"], categories=ordered_buckets, ordered=True)
        subset = subset.sort_values("horizon_bucket")
        label = subset["actual_truth_status"].fillna("NA") + " | " + subset["actual_source"].fillna("NA")
        for key, group in subset.groupby(label):
            ax.plot(group["horizon_bucket"].astype(str), group[value_col], marker="o", label=key)
        ax.set_title(kpi)
        ax.set_ylabel(value_col)
        ax.tick_params(axis="x", rotation=30)
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(PLOTS_DIR / output_name, dpi=150)
    plt.close(fig)


def _plot_forecast_vs_actual_recent(current_eval: pd.DataFrame) -> None:
    if current_eval.empty:
        return
    focus = current_eval[current_eval["kpi"].isin(["entregas_sge", "entregas_sgp", "recogidas_ege", "picking_lines_pi", "picking_units_pi"])].copy()
    if focus.empty:
        return
    fig, axes = plt.subplots(3, 2, figsize=(14, 12), sharex=False)
    axes = axes.flatten()
    for ax, kpi in zip(axes, sorted(focus["kpi"].unique())):
        subset = focus[focus["kpi"] == kpi].sort_values("target_date")
        ax.plot(pd.to_datetime(subset["target_date"]), subset["forecast_value"], label="forecast", marker="o")
        ax.plot(pd.to_datetime(subset["target_date"]), subset["actual_value"], label="actual", marker="x")
        ax.set_title(kpi)
        ax.tick_params(axis="x", rotation=30)
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(PLOTS_DIR / "hybrid_recent_forecast_vs_actual.png", dpi=150)
    plt.close(fig)


def _plot_nowcasting_impact(nowcasting_impact: pd.DataFrame) -> None:
    if nowcasting_impact.empty:
        return
    ordered_buckets = ["D+1", "D+2_a_D+3", "D+4_a_D+7"]
    fig, axes = plt.subplots(1, 3, figsize=(15, 4), sharey=True)
    for ax, kpi in zip(axes, sorted(nowcasting_impact["kpi"].unique())):
        subset = nowcasting_impact[nowcasting_impact["kpi"] == kpi].copy()
        subset["horizon_bucket"] = pd.Categorical(subset["horizon_bucket"], categories=ordered_buckets, ordered=True)
        subset = subset.sort_values("horizon_bucket")
        ax.bar(subset["horizon_bucket"].astype(str), subset["delta_mae"])
        ax.axhline(0, color="black", linewidth=1)
        ax.set_title(kpi)
        ax.set_ylabel("delta_mae")
        ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "hybrid_nowcasting_impact.png", dpi=150)
    plt.close(fig)


def _plot_horizon_coverage(validation_daily: pd.DataFrame) -> None:
    if validation_daily.empty:
        return
    ordered_buckets = ["D+1", "D+2_a_D+3", "D+4_a_D+7", "D+8_a_D+14", "D+15_a_D+28"]
    coverage = validation_daily.groupby(["horizon_bucket", "actual_truth_status"], dropna=False)["n_obs"].sum().reset_index()
    coverage["horizon_bucket"] = pd.Categorical(coverage["horizon_bucket"], categories=ordered_buckets, ordered=True)
    coverage = coverage.sort_values("horizon_bucket")
    fig, ax = plt.subplots(figsize=(10, 4))
    for key, group in coverage.groupby("actual_truth_status", dropna=False):
        ax.bar(group["horizon_bucket"].astype(str), group["n_obs"], label=key, alpha=0.7)
    ax.set_title("Coverage by horizon and actual truth status")
    ax.set_ylabel("n_obs")
    ax.legend()
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "hybrid_horizon_coverage.png", dpi=150)
    plt.close(fig)


def _build_kpi_recommendation(validation_daily: pd.DataFrame, nowcasting_impact: pd.DataFrame) -> str:
    lines = ["# KPI Hybrid Recommendation", ""]
    focus_kpis = ["entregas_sge", "entregas_sgp", "recogidas_ege", "picking_lines_pi", "picking_units_pi"]
    short_buckets = ["D+1", "D+2_a_D+3", "D+4_a_D+7"]
    medium_buckets = ["D+8_a_D+14", "D+15_a_D+28"]
    for kpi in focus_kpis:
        subset = validation_daily[validation_daily["kpi"] == kpi].copy()
        short = subset[subset["horizon_bucket"].isin(short_buckets)]
        medium = subset[subset["horizon_bucket"].isin(medium_buckets)]
        short_wape = short["wape"].mean()
        medium_wape = medium["wape"].mean()
        bias = subset["bias"].mean()
        status = "usar con cautela"
        if np.isfinite(short_wape) and short_wape <= 0.8 and (not np.isfinite(medium_wape) or medium_wape <= 1.0):
            status = "usar tal cual"
        elif np.isfinite(short_wape) and short_wape <= 0.9 and np.isfinite(medium_wape) and medium_wape > 1.0:
            status = "mantener solo corto plazo"
        elif np.isfinite(short_wape) and short_wape > 1.0:
            status = "recalibrar"
        nowcast_note = ""
        if kpi in ["entregas_sge", "entregas_sgp", "recogidas_ege"]:
            impact = nowcasting_impact[nowcasting_impact["kpi"] == kpi]
            labels = impact["label"].value_counts().to_dict()
            if labels.get("helpful", 0) > labels.get("harmful", 0):
                nowcast_note = " Mantener nowcasting corto."
            elif labels.get("harmful", 0) > 0:
                nowcast_note = " Reducir peso de nowcasting."
        bias_note = "sesgo positivo" if np.isfinite(bias) and bias > 0 else ("sesgo negativo" if np.isfinite(bias) and bias < 0 else "sin sesgo claro")
        lines.extend(
            [
                f"## {kpi}",
                f"- Recomendación: {status}",
                f"- Lectura: short_wape={short_wape:.3f} | medium_wape={medium_wape:.3f} | bias={bias:.3f} ({bias_note}).{nowcast_note}",
                "",
            ]
        )
    return "\n".join(lines)


def _build_logic_audit_md(
    validation_daily: pd.DataFrame,
    validation_weekly: pd.DataFrame,
    nowcasting_impact: pd.DataFrame,
    logic_metrics: dict[str, float],
    logic_examples: pd.DataFrame,
) -> str:
    service_daily = validation_daily[validation_daily["kpi"].isin(["entregas_sge", "entregas_sgp", "recogidas_ege"])].copy()
    short = service_daily[service_daily["horizon_bucket"].isin(["D+1", "D+2_a_D+3", "D+4_a_D+7"])]["wape"].mean()
    medium = service_daily[service_daily["horizon_bucket"].isin(["D+8_a_D+14", "D+15_a_D+28"])]["wape"].mean()
    nowcast_labels = nowcasting_impact["label"].value_counts().to_dict()
    lines = [
        "# Hybrid Service Logic Audit",
        "",
        "## Régimen híbrido desde 2026-03-01",
        f"- Rendimiento medio servicios corto plazo: WAPE={short:.3f}",
        f"- Rendimiento medio servicios medio plazo: WAPE={medium:.3f}",
        "- La lectura debe separarse siempre entre `visible_cartera` y `observado_final`; en servicios recientes domina `visible_cartera` por diseño.",
        "",
        "## Lógica actual de solicitudes por codigo_generico",
        f"- % códigos con más de una fecha objetivo observada: {logic_metrics['pct_codigos_multi_fecha']:.2%}",
        f"- % de esos casos donde `min(fecha_objetivo)` difiere de la última fecha conocida: {logic_metrics['pct_multi_fecha_min_diff_latest']:.2%}",
        f"- Desplazamiento mediano `min -> latest`: {logic_metrics['median_shift_days_min_to_latest']:.1f} días",
        f"- P90 desplazamiento `min -> latest`: {logic_metrics['p90_shift_days_min_to_latest']:.1f} días",
        f"- Máximo desplazamiento `min -> latest`: {logic_metrics['max_shift_days_min_to_latest']:.1f} días",
        "",
        "## Riesgo de sesgo por min(fecha_objetivo)",
        "- Si un mismo `codigo_generico` cambia de fecha, usar siempre el mínimo puede adelantar carga visible respecto al estado vigente.",
        "- La corrección natural, si se decide actuar, sería construir el estado vigente por `codigo_generico` usando la última versión conocida por `ultima_modificacion/modificacion_linea/fecha_creacion` y luego agregar.",
        "",
        "## Nowcasting",
        f"- Conteo de diagnóstico `helpful`: {nowcast_labels.get('helpful', 0)}",
        f"- Conteo de diagnóstico `neutral`: {nowcast_labels.get('neutral', 0)}",
        f"- Conteo de diagnóstico `harmful`: {nowcast_labels.get('harmful', 0)}",
        "- El nowcasting se evalúa solo en servicios diarios y especialmente en D+1 a D+7.",
        "",
        "## Utilidad operativa",
        "- El forecast diario se considera utilizable si el corto plazo se mantiene estable y el sesgo no explota por horizonte.",
        "- La lectura de servicios recientes debe entenderse como forecast contra visibilidad operativa, no contra verdad final cerrada.",
        "",
        "## Ejemplos de códigos con cambio de fecha",
        logic_examples[["codigo_generico", "min_fecha", "latest_known_fecha", "max_fecha", "n_fechas", "shift_days_min_to_latest"]].to_string(index=False) if not logic_examples.empty else "- Sin casos relevantes.",
    ]
    return "\n".join(lines)


def main() -> None:
    settings, aliases, regex_rules = load_config()
    ensure_dirs(REPORTS_DIR, PLOTS_DIR)
    context = ensure_processed_context(settings, aliases, regex_rules)
    datasets = load_datasets(settings, aliases, regex_rules)

    daily_detail, nowcast_detail = _replay_recent_daily(settings, datasets, context)
    weekly_detail = _replay_recent_weekly(settings, datasets, context)

    validation_daily = _summarize(daily_detail, "horizon_bucket")
    validation_weekly = _summarize(weekly_detail, "horizon_bucket")
    nowcasting_impact = _build_nowcasting_impact(nowcast_detail)
    logic_metrics, logic_examples = _audit_service_logic()

    validation_daily.to_csv(REPORTS_DIR / "hybrid_regime_validation_daily.csv", index=False)
    validation_weekly.to_csv(REPORTS_DIR / "hybrid_regime_validation_weekly.csv", index=False)
    nowcasting_impact.to_csv(REPORTS_DIR / "nowcasting_impact_report.csv", index=False)
    (REPORTS_DIR / "hybrid_service_logic_audit.md").write_text(
        _build_logic_audit_md(validation_daily, validation_weekly, nowcasting_impact, logic_metrics, logic_examples),
        encoding="utf-8",
    )
    (REPORTS_DIR / "kpi_hybrid_recommendation.md").write_text(
        _build_kpi_recommendation(validation_daily, nowcasting_impact),
        encoding="utf-8",
    )

    current_eval_daily = pd.read_csv(FORECASTS_DIR / "forecast_vs_actual_daily.csv") if (FORECASTS_DIR / "forecast_vs_actual_daily.csv").exists() else pd.DataFrame()
    _plot_daily_metric(validation_daily, "mae", "hybrid_daily_error_by_horizon.png")
    _plot_daily_metric(validation_daily, "bias", "hybrid_daily_bias_by_horizon.png")
    _plot_forecast_vs_actual_recent(current_eval_daily)
    _plot_nowcasting_impact(nowcasting_impact)
    _plot_horizon_coverage(validation_daily)


if __name__ == "__main__":
    main()
