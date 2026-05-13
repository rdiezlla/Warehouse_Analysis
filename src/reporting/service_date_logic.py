from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.paths import CONSUMPTION_DIR, FORECASTS_DIR, PROCESSED_DIR, QA_DIR, REPORTS_DIR
from src.utils.io_utils import save_dataframe

SERVICE_KPI_COLUMN_MAP = {
    "n_entregas_SGE_dia": "entregas_sge",
    "n_entregas_SGP_dia": "entregas_sgp",
    "n_recogidas_EGE_dia": "recogidas_ege",
}

ALBARAN_DELIVERY_FIELDS = [
    "fecha_inicio_evento",
    "inicio_evento",
    "fecha_entrega",
    "reservation_start_date",
]
ALBARAN_PICKUP_FIELDS = [
    "fecha_fin_evento",
    "fin_evento",
    "fecha_recogida",
    "reservation_finish_date",
]
SOLICITUD_DELIVERY_FIELDS = ["fecha_servicio", "fecha_inicio_evento", "reservation_start_date"]
SOLICITUD_PICKUP_FIELDS = ["fecha_servicio", "fecha_fin_evento", "reservation_finish_date"]


def _format_date(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(pd.Timestamp(value).date())


def _available_fields(df: pd.DataFrame, candidates: list[str]) -> list[str]:
    return [field for field in candidates if field in df.columns and df[field].notna().any()]


def build_service_date_logic_summary(albaranes: pd.DataFrame, solicitudes: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for origin, df in [("albaranes", albaranes), ("solicitudes", solicitudes)]:
        if df.empty or "campo_fecha_objetiva_usado" not in df.columns:
            continue
        base = df[
            [
                "tipo_servicio",
                "campo_fecha_objetiva_usado",
                "flag_fecha_objetiva_fallback",
                "flag_fecha_objetiva_missing",
                "fecha_servicio_objetiva",
            ]
        ].copy()
        base["origen_datos"] = origin
        totals = base.groupby(["origen_datos", "tipo_servicio"]).size().rename("total_tipo").reset_index()
        fallback_totals = (
            base.groupby(["origen_datos", "tipo_servicio"])["flag_fecha_objetiva_fallback"]
            .mean()
            .rename("pct_fallback")
            .reset_index()
        )
        missing_totals = (
            base.groupby(["origen_datos", "tipo_servicio"])["flag_fecha_objetiva_missing"]
            .mean()
            .rename("pct_missing")
            .reset_index()
        )
        grouped = (
            base.groupby(["origen_datos", "tipo_servicio", "campo_fecha_objetiva_usado"], dropna=False)
            .agg(
                n_registros=("campo_fecha_objetiva_usado", "size"),
                min_fecha=("fecha_servicio_objetiva", "min"),
                max_fecha=("fecha_servicio_objetiva", "max"),
            )
            .reset_index()
        )
        grouped = grouped.merge(totals, on=["origen_datos", "tipo_servicio"], how="left")
        grouped = grouped.merge(fallback_totals, on=["origen_datos", "tipo_servicio"], how="left")
        grouped = grouped.merge(missing_totals, on=["origen_datos", "tipo_servicio"], how="left")
        grouped["pct_registros_tipo"] = grouped["n_registros"] / grouped["total_tipo"].clip(lower=1)
        rows.extend(grouped.to_dict(orient="records"))
    summary = pd.DataFrame(rows)
    if summary.empty:
        return pd.DataFrame(
            columns=[
                "origen_datos",
                "tipo_servicio",
                "campo_fecha_usado",
                "n_registros",
                "pct_registros_tipo",
                "pct_fallback",
                "pct_missing",
                "min_fecha",
                "max_fecha",
            ]
        )
    summary = summary.rename(columns={"campo_fecha_objetiva_usado": "campo_fecha_usado"})
    summary["min_fecha"] = pd.to_datetime(summary["min_fecha"], errors="coerce")
    summary["max_fecha"] = pd.to_datetime(summary["max_fecha"], errors="coerce")
    return summary.sort_values(["origen_datos", "tipo_servicio", "n_registros"], ascending=[True, True, False])


def write_service_date_logic_reports(albaranes: pd.DataFrame, solicitudes: pd.DataFrame) -> pd.DataFrame:
    summary = build_service_date_logic_summary(albaranes, solicitudes)
    save_dataframe(summary, QA_DIR / "service_date_logic_summary", index=False)

    missing_rows = []
    for origin, df in [("albaranes", albaranes), ("solicitudes", solicitudes)]:
        if "flag_fecha_objetiva_missing" not in df.columns:
            continue
        per_service = (
            df.groupby("tipo_servicio")
            .agg(
                n_registros=("tipo_servicio", "size"),
                n_missing=("flag_fecha_objetiva_missing", "sum"),
                pct_missing=("flag_fecha_objetiva_missing", "mean"),
                pct_fallback=("flag_fecha_objetiva_fallback", "mean"),
            )
            .reset_index()
        )
        per_service["origen_datos"] = origin
        missing_rows.append(per_service)
    missing_detail = pd.concat(missing_rows, ignore_index=True) if missing_rows else pd.DataFrame()

    lines = [
        "# Service Date Logic Audit",
        "",
        "## 1. Logica anterior",
        "- `albaranes` anclaba todos los servicios a `fecha_servicio`.",
        "- `solicitudes` anclaba todos los servicios a `fecha_inicio_evento`.",
        "- Esto implicaba que `EGE` quedaba fechado como si fuera una entrega, no una recogida.",
        "",
        "## 2. Que estaba mal en EGE",
        "- `EGE` representa recogidas, por lo que su fecha objetivo debe ser el fin del evento o la fecha de recogida.",
        "- Usar inicio del evento adelantaba la carga de `EGE` en actuals, cartera, capa hibrida y consumo.",
        "",
        "## 3. Logica nueva",
        "- `SGE` y `SGP`: prioridad a `fecha_servicio` cuando existe; si no, fecha de inicio / entrega.",
        "- `EGE`: prioridad a `fecha_servicio` cuando existe; si no, fecha de fin / recogida.",
        "- La columna operativa comun ahora es `fecha_servicio_objetiva`.",
        "",
        "## 4. Campos usados en albaranes",
        f"- Campos candidatos de entrega presentes: {_available_fields(albaranes, ALBARAN_DELIVERY_FIELDS) or ['<ninguno>']}",
        f"- Campos candidatos de recogida presentes: {_available_fields(albaranes, ALBARAN_PICKUP_FIELDS) or ['<ninguno>']}",
        f"- Campo generico disponible: {['fecha_servicio'] if 'fecha_servicio' in albaranes.columns else ['<ninguno>']}",
        "",
        "## 5. Campos usados en solicitudes",
        f"- Campos candidatos de entrega presentes: {_available_fields(solicitudes, SOLICITUD_DELIVERY_FIELDS) or ['<ninguno>']}",
        f"- Campos candidatos de recogida presentes: {_available_fields(solicitudes, SOLICITUD_PICKUP_FIELDS) or ['<ninguno>']}",
        "",
        "## 6. Fallbacks aplicados",
        "- Albaranes entrega: inicio/entrega preferente, fallback a `fecha_servicio`.",
        "- Albaranes recogida: fin/recogida preferente, fallback a `fecha_servicio`.",
        "- Solicitudes entrega: `fecha_servicio`; fallback a `fecha_inicio_evento` o `reservation_start_date` y despues a fin si no existe inicio.",
        "- Solicitudes recogida: `fecha_servicio`; fallback a `fecha_fin_evento` o `reservation_finish_date` y despues a inicio si no existe fin.",
        "",
        "## 7. Registros sin fecha correcta",
    ]
    if missing_detail.empty:
        lines.append("- No se detectaron registros auditables.")
    else:
        for _, row in missing_detail.iterrows():
            lines.append(
                f"- {row['origen_datos']} | {row['tipo_servicio']}: "
                f"n={int(row['n_registros'])}, pct_fallback={float(row['pct_fallback']):.2%}, "
                f"pct_missing={float(row['pct_missing']):.2%}, n_missing={int(row['n_missing'])}"
            )
    lines.extend(
        [
            "",
            "## Resumen de campo usado",
            summary.to_string(index=False) if not summary.empty else "- Sin resumen disponible.",
        ]
    )
    (REPORTS_DIR / "service_date_logic_audit.md").write_text("\n".join(lines), encoding="utf-8")
    return summary


def _summarize_delta(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    *,
    key_columns: list[str],
    date_column: str,
    value_columns: list[str],
    artifact: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for value_column in value_columns:
        before = before_df[key_columns + [value_column]].copy()
        after = after_df[key_columns + [value_column]].copy()
        before = before.rename(columns={value_column: "value_before"})
        after = after.rename(columns={value_column: "value_after"})
        merged = before.merge(after, on=key_columns, how="outer")
        merged["value_before"] = pd.to_numeric(merged["value_before"], errors="coerce").fillna(0.0)
        merged["value_after"] = pd.to_numeric(merged["value_after"], errors="coerce").fillna(0.0)
        merged[date_column] = pd.to_datetime(merged[date_column], errors="coerce")
        merged["delta"] = merged["value_after"] - merged["value_before"]
        for kpi, group in merged.groupby("kpi", dropna=False):
            nonzero_before = group[group["value_before"] != 0]
            nonzero_after = group[group["value_after"] != 0]
            rows.append(
                {
                    "artifact": artifact,
                    "kpi": kpi,
                    "metric": value_column,
                    "before_total": float(group["value_before"].sum()),
                    "after_total": float(group["value_after"].sum()),
                    "net_delta": float(group["delta"].sum()),
                    "sum_abs_delta": float(group["delta"].abs().sum()),
                    "changed_rows": int(group["delta"].ne(0).sum()),
                    "nonzero_rows_before": int(nonzero_before.shape[0]),
                    "nonzero_rows_after": int(nonzero_after.shape[0]),
                    "min_nonzero_before": _format_date(nonzero_before[date_column].min() if not nonzero_before.empty else pd.NaT),
                    "min_nonzero_after": _format_date(nonzero_after[date_column].min() if not nonzero_after.empty else pd.NaT),
                    "max_nonzero_before": _format_date(nonzero_before[date_column].max() if not nonzero_before.empty else pd.NaT),
                    "max_nonzero_after": _format_date(nonzero_after[date_column].max() if not nonzero_after.empty else pd.NaT),
                }
            )
    return pd.DataFrame(rows)


def maybe_write_service_date_impact_reports() -> pd.DataFrame:
    baseline_dir = QA_DIR / "service_date_logic_baseline"
    required = [
        baseline_dir / "fact_servicio_dia_before.parquet",
        baseline_dir / "forecast_vs_actual_daily_before.csv",
        baseline_dir / "consumo_forecast_diario_before.csv",
    ]
    if not all(path.exists() for path in required):
        return pd.DataFrame()

    comparisons: list[pd.DataFrame] = []

    fact_before = pd.read_parquet(baseline_dir / "fact_servicio_dia_before.parquet")
    fact_after = pd.read_parquet(PROCESSED_DIR / "fact_servicio_dia.parquet")
    fact_before_long = fact_before[["fecha", *SERVICE_KPI_COLUMN_MAP.keys()]].melt(
        id_vars="fecha",
        value_vars=list(SERVICE_KPI_COLUMN_MAP.keys()),
        var_name="metric",
        value_name="value",
    )
    fact_before_long["kpi"] = fact_before_long["metric"].map(SERVICE_KPI_COLUMN_MAP)
    fact_after_long = fact_after[["fecha", *SERVICE_KPI_COLUMN_MAP.keys()]].melt(
        id_vars="fecha",
        value_vars=list(SERVICE_KPI_COLUMN_MAP.keys()),
        var_name="metric",
        value_name="value",
    )
    fact_after_long["kpi"] = fact_after_long["metric"].map(SERVICE_KPI_COLUMN_MAP)
    comparisons.append(
        _summarize_delta(
            fact_before_long.rename(columns={"value": "actual_value"}),
            fact_after_long.rename(columns={"value": "actual_value"}),
            key_columns=["fecha", "kpi"],
            date_column="fecha",
            value_columns=["actual_value"],
            artifact="fact_servicio_dia",
        )
    )

    daily_before = pd.read_csv(baseline_dir / "forecast_vs_actual_daily_before.csv")
    daily_after = pd.read_csv(FORECASTS_DIR / "forecast_vs_actual_daily.csv")
    comparisons.append(
        _summarize_delta(
            daily_before[["target_date", "kpi", "forecast_value", "actual_value"]],
            daily_after[["target_date", "kpi", "forecast_value", "actual_value"]],
            key_columns=["target_date", "kpi"],
            date_column="target_date",
            value_columns=["forecast_value", "actual_value"],
            artifact="forecast_vs_actual_daily",
        )
    )

    if (baseline_dir / "forecast_vs_actual_weekly_before.csv").exists() and (FORECASTS_DIR / "forecast_vs_actual_weekly.csv").exists():
        weekly_before = pd.read_csv(baseline_dir / "forecast_vs_actual_weekly_before.csv")
        weekly_after = pd.read_csv(FORECASTS_DIR / "forecast_vs_actual_weekly.csv")
        comparisons.append(
            _summarize_delta(
                weekly_before[["target_date", "kpi", "forecast_value", "actual_value"]],
                weekly_after[["target_date", "kpi", "forecast_value", "actual_value"]],
                key_columns=["target_date", "kpi"],
                date_column="target_date",
                value_columns=["forecast_value", "actual_value"],
                artifact="forecast_vs_actual_weekly",
            )
        )

    consumo_daily_before = pd.read_csv(baseline_dir / "consumo_forecast_diario_before.csv")
    consumo_daily_after = pd.read_csv(CONSUMPTION_DIR / "consumo_forecast_diario.csv")
    comparisons.append(
        _summarize_delta(
            consumo_daily_before[["fecha", "kpi", "forecast_value", "actual_value"]],
            consumo_daily_after[["fecha", "kpi", "forecast_value", "actual_value"]],
            key_columns=["fecha", "kpi"],
            date_column="fecha",
            value_columns=["forecast_value", "actual_value"],
            artifact="consumo_forecast_diario",
        )
    )

    if (baseline_dir / "consumo_forecast_semanal_before.csv").exists() and (CONSUMPTION_DIR / "consumo_forecast_semanal.csv").exists():
        consumo_weekly_before = pd.read_csv(baseline_dir / "consumo_forecast_semanal_before.csv")
        consumo_weekly_after = pd.read_csv(CONSUMPTION_DIR / "consumo_forecast_semanal.csv")
        comparisons.append(
            _summarize_delta(
                consumo_weekly_before[["week_start_date", "kpi", "forecast_value", "actual_value"]],
                consumo_weekly_after[["week_start_date", "kpi", "forecast_value", "actual_value"]],
                key_columns=["week_start_date", "kpi"],
                date_column="week_start_date",
                value_columns=["forecast_value", "actual_value"],
                artifact="consumo_forecast_semanal",
            )
        )

    comparison_df = pd.concat(comparisons, ignore_index=True) if comparisons else pd.DataFrame()
    save_dataframe(comparison_df, QA_DIR / "service_date_logic_impact_comparison", index=False)

    ege = comparison_df[comparison_df["kpi"] == "recogidas_ege"].copy()
    lines = [
        "# Service Date Logic Impact",
        "",
        "## Artefactos comparados",
        "- `fact_servicio_dia`",
        "- `forecast_vs_actual_daily`",
        "- `forecast_vs_actual_weekly` cuando existia baseline",
        "- `consumo_forecast_diario`",
        "- `consumo_forecast_semanal` cuando existia baseline",
        "",
        "## Impacto en EGE",
        ege.to_string(index=False) if not ege.empty else "- No se detectaron filas para `recogidas_ege`.",
        "",
        "## Comparacion completa",
        comparison_df.to_string(index=False) if not comparison_df.empty else "- Sin comparacion disponible.",
    ]
    (REPORTS_DIR / "service_date_logic_impact.md").write_text("\n".join(lines), encoding="utf-8")
    return comparison_df
