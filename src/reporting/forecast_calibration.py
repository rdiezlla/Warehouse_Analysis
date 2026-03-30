from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.modeling.calibration import (
    apply_daily_postprocess,
    build_baseline_calibration_settings,
    daily_horizon_bucket,
    resolve_nowcasting_weights,
)
from src.modeling.forecast_tracking import build_actuals_daily, build_actuals_weekly
from src.modeling.hierarchical_reconciliation import reconcile_weekly_from_daily
from src.modeling.nowcasting import apply_nowcasting, fit_cartera_scalers
from src.paths import BACKTESTS_DIR, PLOTS_DIR, REPORTS_DIR
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
SHORT_BUCKETS = ["D+1", "D+2_a_D+3", "D+4_a_D+7"]
ALL_BUCKETS = SHORT_BUCKETS + ["D+8_a_D+14", "D+15_a_D+28"]


def _recent_regime_start(settings: dict, context: dict) -> pd.Timestamp:
    configured = settings.get("calibration", {}).get("recent_regime_start")
    if configured:
        return pd.Timestamp(configured).normalize()
    switch_date = context.get("service_layer_meta", {}).get("switch_date")
    return pd.Timestamp(switch_date).normalize() if pd.notna(switch_date) else pd.Timestamp("2026-03-01")


def _safe_wape(actual: pd.Series, abs_error: pd.Series) -> float:
    denom = actual.abs().sum()
    if denom == 0 or pd.isna(denom):
        return np.nan
    return float(abs_error.sum() / denom)


def _safe_bias(error: pd.Series) -> float:
    if error.empty:
        return np.nan
    return float(error.mean())


def _metric_frame(detail_df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []
    for keys, group in detail_df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        valid = group.dropna(subset=["actual_value", "forecast_value"]).copy()
        if valid.empty:
            continue
        valid["signed_error"] = valid["forecast_value"] - valid["actual_value"]
        valid["abs_error"] = valid["signed_error"].abs()
        row["n_obs"] = int(len(valid))
        row["mae"] = float(valid["abs_error"].mean())
        row["wape"] = _safe_wape(valid["actual_value"], valid["abs_error"])
        row["bias"] = _safe_bias(valid["signed_error"])
        row["median_abs_error"] = float(valid["abs_error"].median())
        row["p90_abs_error"] = float(valid["abs_error"].quantile(0.90))
        rows.append(row)
    return pd.DataFrame(rows)


def _replay_recent_daily_variant(settings_variant: dict, datasets: dict[str, pd.DataFrame], context: dict, recent_start: pd.Timestamp) -> pd.DataFrame:
    from src.main import _best_model_name, _current_run_timestamp, _predict_with_model_name

    run_date_end = _current_run_timestamp(settings_variant).tz_localize(None).normalize()
    actuals_daily = build_actuals_daily(context["fact_servicio_dia"], context["fact_picking_dia"])
    metrics = pd.read_csv(BACKTESTS_DIR / "backtest_metrics_all.csv") if (BACKTESTS_DIR / "backtest_metrics_all.csv").exists() else pd.DataFrame()
    scalers = fit_cartera_scalers(context["fact_servicio_dia"], context["fact_cartera"])
    rows: list[pd.DataFrame] = []

    for dataset_name, kpi in MAIN_DATASETS.items():
        df = datasets[dataset_name].copy()
        model_name = _best_model_name(metrics, dataset_name)
        tipo_servicio = str(df["tipo_servicio"].iloc[0])
        for run_date in pd.date_range(recent_start, run_date_end, freq="D"):
            observed = df[(df["is_actual"] == 1) & (pd.to_datetime(df["fecha"]) <= run_date)].copy()
            if observed.empty:
                continue
            future = df[pd.to_datetime(df["fecha"]) > run_date].head(settings_variant["training"]["daily_horizon"]).copy()
            if future.empty:
                continue
            history = observed.set_index(pd.to_datetime(observed["fecha"]))["target"].astype(float)
            forecast = _predict_with_model_name(df, settings_variant, "daily", model_name, history, future).rename(columns={"prediction": "forecast"})
            forecast["target_date"] = pd.to_datetime(forecast["fecha"]).dt.normalize()
            forecast["forecast_run_date"] = run_date
            forecast["horizon_days"] = (forecast["target_date"] - run_date).dt.days.astype(int)
            forecast["dataset_name"] = dataset_name
            forecast["kpi"] = kpi
            forecast["model_name"] = model_name
            forecast["tipo_servicio"] = tipo_servicio
            if dataset_name in SERVICE_DATASETS:
                nowcast_input = forecast[["fecha", "forecast", "tipo_servicio"]].copy()
                adjusted = apply_nowcasting(
                    nowcast_input,
                    context["fact_cartera"],
                    scalers,
                    run_date,
                    settings_variant,
                    maturity_curves=context["cartera_maturity_curves"],
                )
                forecast = forecast.merge(
                    adjusted[
                        [
                            "fecha",
                            "forecast",
                            "cartera_signal",
                            "statistical_weight",
                            "cartera_weight",
                            "horizon_bucket",
                        ]
                    ].rename(columns={"forecast": "forecast_after_nowcasting"}),
                    on="fecha",
                    how="left",
                )
                forecast["forecast_value"] = forecast["forecast_after_nowcasting"].fillna(forecast["forecast"])
                forecast["forecast_source"] = "service_model"
            else:
                forecast["forecast_value"] = forecast["forecast"]
                forecast["forecast_source"] = "picking_model"
                forecast["cartera_signal"] = np.nan
                forecast["statistical_weight"] = np.nan
                forecast["cartera_weight"] = np.nan
                forecast["horizon_bucket"] = forecast["horizon_days"].map(daily_horizon_bucket)
            rows.append(
                forecast[
                    [
                        "forecast_run_date",
                        "target_date",
                        "horizon_days",
                        "dataset_name",
                        "kpi",
                        "model_name",
                        "tipo_servicio",
                        "forecast",
                        "forecast_value",
                        "forecast_source",
                        "cartera_signal",
                        "statistical_weight",
                        "cartera_weight",
                        "horizon_bucket",
                    ]
                ]
            )

    detail = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if detail.empty:
        return detail

    detail = detail.rename(columns={"forecast": "forecast_base"})
    detail = apply_daily_postprocess(
        detail.rename(columns={"forecast_value": "forecast"}),
        settings_variant,
    ).rename(columns={"forecast": "forecast_value"})
    detail = detail.merge(actuals_daily, on=["target_date", "kpi"], how="left")
    detail["horizon_bucket"] = detail["horizon_days"].map(daily_horizon_bucket)
    detail["signed_error"] = detail["forecast_value"] - detail["actual_value"]
    detail["abs_error"] = detail["signed_error"].abs()
    detail = detail[(detail["target_date"] >= recent_start) & detail["horizon_bucket"].notna()].copy()
    return detail


def _aggregate_weekly_official(detail_df: pd.DataFrame, context: dict) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame(columns=["forecast_run_date", "target_date", "kpi", "forecast_value", "actual_value"])
    actuals_weekly = build_actuals_weekly(build_actuals_daily(context["fact_servicio_dia"], context["fact_picking_dia"]))
    rows = []
    for run_date, group in detail_df.groupby("forecast_run_date", dropna=False):
        weekly = reconcile_weekly_from_daily(
            group.rename(columns={"target_date": "fecha", "forecast_value": "forecast"})[["fecha", "kpi", "forecast"]]
        ).rename(columns={"fecha": "target_date", "forecast": "forecast_value"})
        weekly["forecast_run_date"] = pd.Timestamp(run_date).normalize()
        rows.append(weekly)
    weekly_detail = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if weekly_detail.empty:
        return weekly_detail
    weekly_detail = weekly_detail.merge(actuals_weekly, on=["target_date", "kpi"], how="left")
    weekly_detail["signed_error"] = weekly_detail["forecast_value"] - weekly_detail["actual_value"]
    weekly_detail["abs_error"] = weekly_detail["signed_error"].abs()
    return weekly_detail


def _build_daily_report(before_detail: pd.DataFrame, after_detail: pd.DataFrame) -> pd.DataFrame:
    before_metrics = _metric_frame(before_detail, ["kpi", "horizon_bucket"]).rename(
        columns={
            "n_obs": "n_obs_before",
            "mae": "mae_before",
            "wape": "wape_before",
            "bias": "bias_before",
            "median_abs_error": "median_abs_error_before",
            "p90_abs_error": "p90_abs_error_before",
        }
    )
    after_metrics = _metric_frame(after_detail, ["kpi", "horizon_bucket"]).rename(
        columns={
            "n_obs": "n_obs_after",
            "mae": "mae_after",
            "wape": "wape_after",
            "bias": "bias_after",
            "median_abs_error": "median_abs_error_after",
            "p90_abs_error": "p90_abs_error_after",
        }
    )
    report = before_metrics.merge(after_metrics, on=["kpi", "horizon_bucket"], how="outer")
    report["delta_mae"] = report["mae_after"] - report["mae_before"]
    report["delta_wape"] = report["wape_after"] - report["wape_before"]
    report["delta_bias"] = report["bias_after"] - report["bias_before"]
    ordered = pd.Categorical(report["horizon_bucket"], categories=ALL_BUCKETS, ordered=True)
    return report.assign(_order=ordered).sort_values(["kpi", "_order"]).drop(columns="_order")


def _build_weekly_report(before_weekly: pd.DataFrame, after_weekly: pd.DataFrame) -> pd.DataFrame:
    before_metrics = _metric_frame(before_weekly, ["kpi"]).rename(
        columns={
            "n_obs": "n_obs_before",
            "mae": "mae_before",
            "wape": "wape_before",
            "bias": "bias_before",
            "median_abs_error": "median_abs_error_before",
            "p90_abs_error": "p90_abs_error_before",
        }
    )
    after_metrics = _metric_frame(after_weekly, ["kpi"]).rename(
        columns={
            "n_obs": "n_obs_after",
            "mae": "mae_after",
            "wape": "wape_after",
            "bias": "bias_after",
            "median_abs_error": "median_abs_error_after",
            "p90_abs_error": "p90_abs_error_after",
        }
    )
    report = before_metrics.merge(after_metrics, on=["kpi"], how="outer")
    report["delta_mae"] = report["mae_after"] - report["mae_before"]
    report["delta_wape"] = report["wape_after"] - report["wape_before"]
    report["delta_bias"] = report["bias_after"] - report["bias_before"]
    return report.sort_values("kpi")


def _build_nowcasting_calibration_report(before_detail: pd.DataFrame, after_detail: pd.DataFrame, settings: dict) -> pd.DataFrame:
    before_metrics = _metric_frame(
        before_detail[before_detail["kpi"].isin(SERVICE_DATASETS.values())].copy(),
        ["kpi", "horizon_bucket"],
    ).rename(
        columns={
            "mae": "mae_before",
            "wape": "wape_before",
            "bias": "bias_before",
        }
    )
    after_metrics = _metric_frame(
        after_detail[after_detail["kpi"].isin(SERVICE_DATASETS.values())].copy(),
        ["kpi", "horizon_bucket"],
    ).rename(
        columns={
            "mae": "mae_after",
            "wape": "wape_after",
            "bias": "bias_after",
        }
    )
    report = before_metrics.merge(after_metrics, on=["kpi", "horizon_bucket"], how="outer")
    report["tipo_servicio"] = report["kpi"].map(
        {
            "entregas_sge": "SGE",
            "entregas_sgp": "SGP",
            "recogidas_ege": "EGE",
        }
    )
    weights = report.apply(
        lambda row: pd.Series(
            resolve_nowcasting_weights(
                settings,
                row["tipo_servicio"],
                {"D+1": 1, "D+2_a_D+3": 2, "D+4_a_D+7": 4, "D+8_a_D+14": 8, "D+15_a_D+28": 15}.get(row["horizon_bucket"], 29),
            )
        ),
        axis=1,
    )
    report["statistical_weight"] = weights["statistical_weight"]
    report["cartera_weight"] = weights["cartera_weight"]
    report["delta_mae"] = report["mae_after"] - report["mae_before"]
    report["delta_wape"] = report["wape_after"] - report["wape_before"]
    report["delta_bias"] = report["bias_after"] - report["bias_before"]
    report["impact_label"] = np.where(
        (report["delta_mae"] <= -0.05) | (report["delta_wape"] <= -0.02),
        "helpful",
        np.where(
            (report["delta_mae"] >= 0.05) | (report["delta_wape"] >= 0.02),
            "harmful",
            "neutral",
        ),
    )
    report["decision_final"] = report["kpi"].map(
        {
            "entregas_sge": "active",
            "entregas_sgp": "active",
            "recogidas_ege": "disabled",
        }
    )
    ordered = pd.Categorical(report["horizon_bucket"], categories=ALL_BUCKETS, ordered=True)
    return report.assign(_order=ordered).sort_values(["kpi", "_order"]).drop(columns="_order")


def _plot_daily_before_after(report: pd.DataFrame, value_before: str, value_after: str, output_path: Path, title: str) -> None:
    if report.empty:
        return
    fig, axes = plt.subplots(3, 2, figsize=(14, 12), sharex=True)
    axes = axes.flatten()
    for ax, kpi in zip(axes, sorted(report["kpi"].unique())):
        subset = report[report["kpi"] == kpi].copy()
        subset["horizon_bucket"] = pd.Categorical(subset["horizon_bucket"], categories=ALL_BUCKETS, ordered=True)
        subset = subset.sort_values("horizon_bucket")
        ax.plot(subset["horizon_bucket"].astype(str), subset[value_before], marker="o", label="before")
        ax.plot(subset["horizon_bucket"].astype(str), subset[value_after], marker="o", label="after")
        ax.set_title(kpi)
        ax.tick_params(axis="x", rotation=30)
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.suptitle(title)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def _plot_weekly_before_after(report: pd.DataFrame, output_path: Path) -> None:
    if report.empty:
        return
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))
    metrics = [("mae_before", "mae_after", "MAE"), ("wape_before", "wape_after", "WAPE"), ("bias_before", "bias_after", "Bias")]
    x = np.arange(len(report))
    width = 0.35
    for ax, (before_col, after_col, label) in zip(axes, metrics):
        ax.bar(x - width / 2, report[before_col], width, label="before")
        ax.bar(x + width / 2, report[after_col], width, label="after")
        ax.set_title(label)
        ax.set_xticks(x)
        ax.set_xticklabels(report["kpi"], rotation=30, ha="right")
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def _plot_nowcasting_impact(report: pd.DataFrame, output_path: Path) -> None:
    if report.empty:
        return
    short = report[report["horizon_bucket"].isin(SHORT_BUCKETS)].copy()
    if short.empty:
        return
    fig, axes = plt.subplots(1, 3, figsize=(15, 4), sharey=True)
    for ax, kpi in zip(axes, sorted(short["kpi"].unique())):
        subset = short[short["kpi"] == kpi].copy()
        subset["horizon_bucket"] = pd.Categorical(subset["horizon_bucket"], categories=SHORT_BUCKETS, ordered=True)
        subset = subset.sort_values("horizon_bucket")
        ax.bar(subset["horizon_bucket"].astype(str), subset["delta_mae"])
        ax.axhline(0, color="black", linewidth=1)
        ax.set_title(kpi)
        ax.set_ylabel("delta_mae")
        ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def _plot_recent_examples(after_detail: pd.DataFrame, output_path: Path) -> None:
    if after_detail.empty:
        return
    eligible = after_detail.groupby("forecast_run_date")["target_date"].max().reset_index()
    latest_complete_run = eligible["forecast_run_date"].max()
    focus = after_detail[
        (after_detail["forecast_run_date"] == latest_complete_run)
        & (after_detail["horizon_days"].between(1, 7))
    ].copy()
    if focus.empty:
        return
    fig, axes = plt.subplots(3, 2, figsize=(14, 12))
    axes = axes.flatten()
    for ax, kpi in zip(axes, sorted(focus["kpi"].unique())):
        subset = focus[focus["kpi"] == kpi].sort_values("target_date")
        ax.plot(subset["target_date"], subset["forecast_value"], marker="o", label="forecast")
        ax.plot(subset["target_date"], subset["actual_value"], marker="x", label="actual")
        ax.set_title(kpi)
        ax.tick_params(axis="x", rotation=30)
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def _build_operational_policy_md(daily_report: pd.DataFrame, weekly_report: pd.DataFrame) -> str:
    lines = [
        "# KPI Operational Usage Policy",
        "",
        "- Forecast maestro operativo: diario calibrado.",
        "- Semanal oficial: agregado del diario calibrado.",
        "- Weekly direct sigue fuera de operaci\u00f3n y solo se usa como benchmark diagn\u00f3stico.",
        "",
    ]
    for kpi in ["entregas_sge", "entregas_sgp", "recogidas_ege", "picking_lines_pi", "picking_units_pi"]:
        subset = daily_report[daily_report["kpi"] == kpi]
        short = subset[subset["horizon_bucket"].isin(SHORT_BUCKETS)]
        medium = subset[subset["horizon_bucket"].isin(["D+8_a_D+14", "D+15_a_D+28"])]
        short_wape = short["wape_after"].mean()
        medium_wape = medium["wape_after"].mean()
        recommendation = "usar con cautela"
        if np.isfinite(short_wape) and short_wape <= 0.65:
            recommendation = "usar diario D+1..D+7"
        if np.isfinite(short_wape) and short_wape <= 0.8 and np.isfinite(medium_wape) and medium_wape > 1.0:
            recommendation = "usar diario D+1..D+7 y semanal oficial como vision tactica"
        if kpi == "recogidas_ege":
            recommendation = "usar con cautela y priorizar D+1..D+7"
        if kpi.startswith("picking"):
            recommendation = "usar diario D+1..D+14 y semanal oficial como vision tactica"
        lines.extend(
            [
                f"## {kpi}",
                f"- Recomendacion: {recommendation}",
                f"- WAPE diario corto plazo despues de calibracion: {short_wape:.3f}" if np.isfinite(short_wape) else "- WAPE diario corto plazo despues de calibracion: NA",
                f"- WAPE diario medio plazo despues de calibracion: {medium_wape:.3f}" if np.isfinite(medium_wape) else "- WAPE diario medio plazo despues de calibracion: NA",
                "",
            ]
        )
    lines.extend(
        [
            "## Semanal oficial",
            "- Consumir solo `weekly_forecasts.csv`.",
            "- Interpretarlo como suma coherente del diario calibrado, no como un modelo independiente.",
            "",
        ]
    )
    return "\n".join(lines)


def _build_audit_md(daily_report: pd.DataFrame, weekly_report: pd.DataFrame, nowcasting_report: pd.DataFrame) -> str:
    lines = [
        "# Forecast Calibration Audit",
        "",
        "## Que se calibro",
        "- Pesos de nowcasting por KPI y bucket corto en servicios.",
        "- Postprocesado minimo de salida diaria en KPIs donde mejoraba el error real.",
        "- Validacion del semanal oficial como agregado del diario calibrado.",
        "",
        "## Que no se toco",
        "- Arquitectura de modelos.",
        "- Capa hibrida de servicios.",
        "- Historico de emisiones y `forecast_vs_actual`.",
        "- Politica semanal oficial vs diagnostico.",
        "",
        "## Cambios activos",
        "- `entregas_sge`: nowcasting fuerte en D+1 y D+2..D+3; moderado en D+4..D+7; redondeo final.",
        "- `entregas_sgp`: nowcasting fuerte en D+1, intermedio en D+2..D+3, moderado en D+4..D+7; sin redondeo final.",
        "- `recogidas_ege`: nowcasting corto desactivado; redondeo final.",
        "- `picking_lines_pi`: sin sesgo aditivo; redondeo final.",
        "- `picking_units_pi`: ajuste aditivo de +500 unidades y redondeo final.",
        "",
        "## Mejora observada",
    ]
    for kpi in ["entregas_sge", "entregas_sgp", "recogidas_ege", "picking_lines_pi", "picking_units_pi"]:
        subset = daily_report[(daily_report["kpi"] == kpi) & (daily_report["horizon_bucket"].isin(SHORT_BUCKETS))]
        before_wape = subset["wape_before"].mean()
        after_wape = subset["wape_after"].mean()
        before_bias = subset["bias_before"].mean()
        after_bias = subset["bias_after"].mean()
        lines.append(
            f"- `{kpi}`: WAPE corto {before_wape:.3f} -> {after_wape:.3f}; bias corto {before_bias:.3f} -> {after_bias:.3f}."
            if np.isfinite(before_wape) and np.isfinite(after_wape)
            else f"- `{kpi}`: sin evidencia suficiente."
        )
    lines.extend(
        [
            "",
            "## KPIs mas estables",
            "- `entregas_sge`, `entregas_sgp`, `picking_lines_pi` y `picking_units_pi` quedan en una zona operativamente util.",
            "- `recogidas_ege` mejora al quitar nowcasting, pero sigue siendo el KPI mas delicado.",
            "",
            "## Politica semanal final",
            "- El semanal oficial sigue siendo solo el agregado del diario calibrado.",
            "- No se aplica una calibracion semanal independiente para no romper coherencia con el maestro diario.",
            "",
            "## Recomendacion al equipo",
            "- Operacion diaria: usar principalmente D+1..D+7 en servicios.",
            "- Picking: usar diario y semanal oficial agregado.",
            "- `recogidas_ege`: consumir con cautela y revisar sesgo con seguimiento real.",
            "",
            "## Nowcasting",
        ]
    )
    for kpi in ["entregas_sge", "entregas_sgp", "recogidas_ege"]:
        subset = nowcasting_report[nowcasting_report["kpi"] == kpi]
        helpful = int((subset["impact_label"] == "helpful").sum())
        harmful = int((subset["impact_label"] == "harmful").sum())
        lines.append(f"- `{kpi}`: helpful={helpful}, harmful={harmful}, decision={subset['decision_final'].iloc[0] if not subset.empty else 'NA'}.")
    return "\n".join(lines)


def generate_calibration_artifacts(settings: dict, context: dict, datasets: dict[str, pd.DataFrame]) -> None:
    ensure_dirs(REPORTS_DIR, PLOTS_DIR)
    recent_start = _recent_regime_start(settings, context)
    before_settings = build_baseline_calibration_settings(settings)
    after_settings = settings

    LOGGER.info("Generating final calibration artifacts from %s", recent_start.date())
    before_daily = _replay_recent_daily_variant(before_settings, datasets, context, recent_start)
    after_daily = _replay_recent_daily_variant(after_settings, datasets, context, recent_start)
    before_weekly = _aggregate_weekly_official(before_daily, context)
    after_weekly = _aggregate_weekly_official(after_daily, context)

    daily_report = _build_daily_report(before_daily, after_daily)
    weekly_report = _build_weekly_report(before_weekly, after_weekly)
    nowcasting_report = _build_nowcasting_calibration_report(before_daily, after_daily, settings)

    daily_report.to_csv(REPORTS_DIR / "calibration_daily_report.csv", index=False)
    weekly_report.to_csv(REPORTS_DIR / "calibration_weekly_report.csv", index=False)
    nowcasting_report.to_csv(REPORTS_DIR / "nowcasting_calibration_report.csv", index=False)
    (REPORTS_DIR / "forecast_calibration_audit.md").write_text(_build_audit_md(daily_report, weekly_report, nowcasting_report), encoding="utf-8")
    (REPORTS_DIR / "kpi_operational_usage_policy.md").write_text(_build_operational_policy_md(daily_report, weekly_report), encoding="utf-8")

    _plot_daily_before_after(
        daily_report,
        "wape_before",
        "wape_after",
        PLOTS_DIR / "calibration_daily_before_after.png",
        "Daily WAPE before vs after calibration",
    )
    _plot_daily_before_after(
        daily_report,
        "bias_before",
        "bias_after",
        PLOTS_DIR / "calibration_daily_bias_before_after.png",
        "Daily bias before vs after calibration",
    )
    _plot_nowcasting_impact(nowcasting_report, PLOTS_DIR / "calibration_nowcasting_before_after.png")
    _plot_weekly_before_after(weekly_report, PLOTS_DIR / "calibration_weekly_before_after.png")
    _plot_recent_examples(after_daily, PLOTS_DIR / "calibration_recent_forecast_vs_actual.png")
