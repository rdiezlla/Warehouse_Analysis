from __future__ import annotations

from pathlib import Path
import sys
from typing import Any

import pandas as pd
import json


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUTPUTS_DIR = ROOT / "outputs"
DEBUG_DIR = OUTPUTS_DIR / "debug"

PICKING_KPIS = ["picking_lines_pi", "picking_units_pi"]
FACT_PICKING_COLUMNS = ["picking_lines_PI_dia", "picking_units_PI_dia"]


def _status_label(status: str) -> str:
    return status.upper().ljust(7)


class AuditLog:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self.rows: list[dict[str, Any]] = []

    def add(self, status: str, check: str, detail: str, **metrics: Any) -> None:
        self.lines.append(f"[{_status_label(status)}] {check}: {detail}")
        row = {"status": status.upper(), "check": check, "detail": detail}
        row.update(metrics)
        self.rows.append(row)

    def section(self, title: str) -> None:
        self.lines.append("")
        self.lines.append(f"## {title}")

    def write(self) -> None:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        report_path = DEBUG_DIR / "picking_2024_audit.txt"
        summary_path = DEBUG_DIR / "picking_2024_audit_summary.csv"
        report_path.write_text("\n".join(self.lines).strip() + "\n", encoding="utf-8")
        pd.DataFrame(self.rows).to_csv(summary_path, index=False)
        print("\n".join(self.lines))
        print("")
        print(f"Report written to: {report_path}")
        print(f"Summary written to: {summary_path}")


def load_table(base_path: Path, audit: AuditLog, label: str) -> pd.DataFrame | None:
    parquet_path = base_path.with_suffix(".parquet")
    csv_path = base_path.with_suffix(".csv")

    if parquet_path.exists():
        try:
            df = pd.read_parquet(parquet_path)
            audit.add(
                "OK",
                f"load_{label}",
                f"Loaded parquet {parquet_path} with {len(df):,} rows",
                path=str(parquet_path),
                rows=len(df),
            )
            return df
        except Exception as exc:  # pragma: no cover - diagnostic script
            audit.add("WARNING", f"load_{label}", f"Could not read parquet: {exc}", path=str(parquet_path))

    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path)
            audit.add(
                "OK",
                f"load_{label}",
                f"Loaded csv {csv_path} with {len(df):,} rows",
                path=str(csv_path),
                rows=len(df),
            )
            return df
        except Exception as exc:  # pragma: no cover - diagnostic script
            audit.add("ERROR", f"load_{label}", f"Could not read csv: {exc}", path=str(csv_path))
            return None

    audit.add("WARNING", f"load_{label}", f"Missing artifact {parquet_path} or {csv_path}")
    return None


def to_datetime_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def pct(value: int | float, denominator: int | float) -> float:
    if denominator == 0:
        return 0.0
    return float(value) / float(denominator)


def save_sample(df: pd.DataFrame, path: Path, audit: AuditLog, label: str, max_rows: int = 10) -> None:
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    sample = df.head(max_rows).copy()
    sample.to_csv(path, index=False)
    audit.add("OK", label, f"Saved sample with {len(sample):,} rows to {path}", path=str(path), rows=len(sample))


def inspect_source_file(audit: AuditLog) -> None:
    audit.section("Source file selected by ingestion")
    configured = "movimientos.xlsx"
    source_candidates = [ROOT / configured, DATA_DIR / "raw" / configured]
    selected_source = next((path for path in source_candidates if path.exists()), None)
    raw_path = DATA_DIR / "raw" / configured

    if selected_source is None:
        audit.add("ERROR", "movimientos_source", "No movimientos.xlsx found in repo root or data/raw")
        return

    audit.add(
        "OK",
        "movimientos_source",
        "ingest.locate_source_files prefers repo root, then data/raw",
        selected_source=str(selected_source),
        runtime_read_path=str(raw_path),
    )
    for path in source_candidates:
        if path.exists():
            stat = path.stat()
            audit.add(
                "OK",
                f"file_{path.parent.name}_{path.name}",
                f"Exists, size={stat.st_size:,} bytes, modified={pd.Timestamp(stat.st_mtime, unit='s')}",
                path=str(path),
                bytes=stat.st_size,
            )
        else:
            audit.add("WARNING", f"file_{path.parent.name}_{path.name}", "Missing", path=str(path))

    if selected_source.exists() and raw_path.exists() and selected_source.stat().st_size != raw_path.stat().st_size:
        audit.add(
            "WARNING",
            "movimientos_source_mismatch",
            "Repo root and data/raw copies have different sizes",
            source_bytes=selected_source.stat().st_size,
            raw_bytes=raw_path.stat().st_size,
        )


def inspect_movimientos_clean(df: pd.DataFrame | None, audit: AuditLog) -> None:
    audit.section("movimientos_clean")
    if df is None:
        audit.add("ERROR", "movimientos_clean_available", "Cannot inspect missing movimientos_clean")
        return

    required = {"tipo_movimiento", "fecha_operativa_mov", "pedido_externo", "cantidad"}
    missing = required - set(df.columns)
    if missing:
        audit.add("ERROR", "movimientos_clean_columns", f"Missing required columns: {sorted(missing)}")
        return

    work = df.copy()
    work["fecha_operativa_mov"] = to_datetime_series(work["fecha_operativa_mov"])
    work["year"] = work["fecha_operativa_mov"].dt.year
    pi = work[work["tipo_movimiento"].astype(str).str.upper() == "PI"].copy()
    pi_2024 = pi[pi["year"] == 2024].copy()

    audit.add(
        "OK",
        "movimientos_clean_date_range",
        f"fecha_operativa_mov range {work['fecha_operativa_mov'].min()} -> {work['fecha_operativa_mov'].max()}",
        min_fecha=work["fecha_operativa_mov"].min(),
        max_fecha=work["fecha_operativa_mov"].max(),
    )
    audit.add("OK", "movimientos_clean_pi_total", f"PI rows: {len(pi):,}", rows=len(pi))
    audit.add("OK", "movimientos_clean_pi_2024", f"PI rows in 2024: {len(pi_2024):,}", rows=len(pi_2024))
    audit.add(
        "OK" if pi["fecha_operativa_mov"].notna().all() else "WARNING",
        "movimientos_clean_pi_null_fecha",
        f"PI null fecha_operativa_mov: {pi['fecha_operativa_mov'].isna().mean():.2%}",
        pct_missing=pi["fecha_operativa_mov"].isna().mean(),
    )
    audit.add(
        "OK" if pi["pedido_externo"].notna().all() else "WARNING",
        "movimientos_clean_pi_null_pedido",
        f"PI null pedido_externo: {pi['pedido_externo'].isna().mean():.2%}",
        pct_missing=pi["pedido_externo"].isna().mean(),
    )

    year_dist = pi.groupby("year", dropna=False).size().reset_index(name="n_rows").sort_values("year")
    year_dist_path = DEBUG_DIR / "picking_2024_movimientos_year_distribution.csv"
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    year_dist.to_csv(year_dist_path, index=False)
    audit.add("OK", "movimientos_clean_pi_year_distribution", f"Saved year distribution to {year_dist_path}", path=str(year_dist_path))

    sample_columns = [
        column
        for column in [
            "fecha_inicio",
            "fecha_finalizacion",
            "fecha_operativa_mov",
            "tipo_movimiento",
            "pedido_externo",
            "cantidad",
        ]
        if column in pi_2024.columns
    ]
    save_sample(pi_2024[sample_columns], DEBUG_DIR / "picking_2024_movimientos_sample.csv", audit, "movimientos_clean_pi_2024_sample")

    variants = work["tipo_movimiento"].astype(str).value_counts(dropna=False).head(20)
    variants_path = DEBUG_DIR / "picking_2024_tipo_movimiento_values.csv"
    variants.rename_axis("tipo_movimiento").reset_index(name="n_rows").to_csv(variants_path, index=False)
    audit.add("OK", "movimientos_clean_tipo_movimiento_values", f"Saved top tipo_movimiento values to {variants_path}", path=str(variants_path))


def inspect_fact_picking(df: pd.DataFrame | None, audit: AuditLog) -> None:
    audit.section("fact_picking_dia")
    if df is None:
        audit.add("ERROR", "fact_picking_available", "Cannot inspect missing fact_picking_dia")
        return

    required = {"fecha", *FACT_PICKING_COLUMNS}
    missing = required - set(df.columns)
    if missing:
        audit.add("ERROR", "fact_picking_columns", f"Missing required columns: {sorted(missing)}")
        return

    work = df.copy()
    work["fecha"] = to_datetime_series(work["fecha"])
    work["year"] = work["fecha"].dt.year
    rows_2024 = work[work["year"] == 2024].copy()

    audit.add(
        "OK",
        "fact_picking_date_range",
        f"fecha range {work['fecha'].min()} -> {work['fecha'].max()}",
        min_fecha=work["fecha"].min(),
        max_fecha=work["fecha"].max(),
    )
    audit.add("OK", "fact_picking_rows_2024", f"Rows in 2024: {len(rows_2024):,}", rows=len(rows_2024))
    for column in FACT_PICKING_COLUMNS:
        total = rows_2024[column].sum()
        audit.add("OK" if total > 0 else "WARNING", f"fact_picking_2024_sum_{column}", f"2024 sum {column}: {total:,.0f}", total=total)

    sample_columns = ["fecha", *FACT_PICKING_COLUMNS]
    save_sample(rows_2024[sample_columns], DEBUG_DIR / "picking_2024_fact_picking_sample.csv", audit, "fact_picking_2024_sample")


def inspect_reconstructed_actuals(fact_picking: pd.DataFrame | None, audit: AuditLog) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    audit.section("reconstructed actuals_daily / actuals_weekly")
    if fact_picking is None:
        audit.add("ERROR", "actuals_reconstruct", "Cannot reconstruct actuals without fact_picking_dia")
        return None, None

    try:
        sys.path.insert(0, str(ROOT))
        from src.modeling.forecast_tracking import build_actuals_weekly, build_picking_actual_daily

        daily = build_picking_actual_daily(fact_picking)
        weekly = build_actuals_weekly(daily)
    except Exception as exc:  # pragma: no cover - diagnostic script
        audit.add("ERROR", "actuals_reconstruct", f"Could not reconstruct actuals: {exc}")
        return None, None

    daily["target_date"] = to_datetime_series(daily["target_date"])
    weekly["target_date"] = to_datetime_series(weekly["target_date"])

    for label, actuals in [("daily", daily), ("weekly", weekly)]:
        actuals["year"] = actuals["target_date"].dt.year
        for kpi in PICKING_KPIS:
            subset_2024 = actuals[(actuals["kpi"] == kpi) & (actuals["year"] == 2024)]
            nonzero = int((subset_2024["actual_value"] > 0).sum()) if not subset_2024.empty else 0
            total = float(subset_2024["actual_value"].sum()) if not subset_2024.empty else 0.0
            audit.add(
                "OK" if total > 0 else "WARNING",
                f"actuals_{label}_2024_{kpi}",
                f"{label} 2024 rows={len(subset_2024):,}, nonzero={nonzero:,}, sum={total:,.0f}",
                rows=len(subset_2024),
                nonzero=nonzero,
                total=total,
            )

    return daily, weekly


def inspect_consumption_table(df: pd.DataFrame | None, audit: AuditLog, label: str, date_column: str) -> None:
    audit.section(label)
    if df is None:
        audit.add("ERROR", f"{label}_available", f"Cannot inspect missing {label}")
        return

    required = {date_column, "kpi", "actual_value_2024"}
    missing = required - set(df.columns)
    if missing:
        audit.add("ERROR", f"{label}_columns", f"Missing required columns: {sorted(missing)}")
        return

    work = df.copy()
    work[date_column] = to_datetime_series(work[date_column])
    work["year"] = work[date_column].dt.year
    audit.add(
        "OK",
        f"{label}_date_range",
        f"{date_column} range {work[date_column].min()} -> {work[date_column].max()}",
        min_fecha=work[date_column].min(),
        max_fecha=work[date_column].max(),
    )

    for kpi in PICKING_KPIS:
        subset = work[work["kpi"] == kpi].copy()
        if subset.empty:
            audit.add("ERROR", f"{label}_{kpi}", "No rows for KPI", kpi=kpi)
            continue
        nulls = int(subset["actual_value_2024"].isna().sum())
        non_null = int(subset["actual_value_2024"].notna().sum())
        total = float(subset["actual_value_2024"].sum(skipna=True))
        status = "OK" if non_null > 0 else "ERROR"
        audit.add(
            status,
            f"{label}_{kpi}_coverage",
            f"rows={len(subset):,}, non_null_2024={non_null:,}, null_2024={nulls:,}, sum_2024={total:,.0f}",
            rows=len(subset),
            non_null_2024=non_null,
            null_2024=nulls,
            pct_missing=pct(nulls, len(subset)),
            sum_2024=total,
        )

    sample = work[work["kpi"].isin(PICKING_KPIS)].head(20)
    save_sample(sample, DEBUG_DIR / f"picking_2024_{label}_sample.csv", audit, f"{label}_picking_sample", max_rows=20)


def inspect_forecast_reference_alignment(
    forecast_df: pd.DataFrame | None,
    vs_2024_df: pd.DataFrame | None,
    audit: AuditLog,
    label: str,
    date_column: str,
) -> None:
    audit.section(f"{label} forecast vs 2024 alignment")
    if forecast_df is None or vs_2024_df is None:
        audit.add("ERROR", f"{label}_alignment_available", "Cannot inspect alignment without forecast and vs_2024 tables")
        return

    required_forecast = {date_column, "kpi", "forecast_value"}
    required_vs = {date_column, "kpi", "actual_value_2024"}
    missing_forecast = required_forecast - set(forecast_df.columns)
    missing_vs = required_vs - set(vs_2024_df.columns)
    if missing_forecast or missing_vs:
        audit.add(
            "ERROR",
            f"{label}_alignment_columns",
            f"Missing forecast columns {sorted(missing_forecast)} or vs columns {sorted(missing_vs)}",
        )
        return

    forecast = forecast_df.copy()
    vs_2024 = vs_2024_df.copy()
    forecast[date_column] = to_datetime_series(forecast[date_column])
    vs_2024[date_column] = to_datetime_series(vs_2024[date_column])
    forecast["forecast_value"] = pd.to_numeric(forecast["forecast_value"], errors="coerce")
    vs_2024["actual_value_2024"] = pd.to_numeric(vs_2024["actual_value_2024"], errors="coerce")
    forecast["quarter"] = forecast[date_column].dt.quarter
    vs_2024["quarter"] = vs_2024[date_column].dt.quarter

    selected_quarters = sorted(forecast.loc[forecast["kpi"].isin(PICKING_KPIS), "quarter"].dropna().astype(int).unique().tolist())
    audit.add(
        "OK" if selected_quarters else "WARNING",
        f"{label}_frontend_default_quarters_from_forecast",
        f"Forecast picking rows imply default quarters {selected_quarters}",
        selected_quarters=",".join(map(str, selected_quarters)),
    )

    rows = []
    for kpi in PICKING_KPIS:
        for quarter in selected_quarters:
            forecast_subset = forecast[(forecast["kpi"] == kpi) & (forecast["quarter"] == quarter)]
            vs_subset = vs_2024[(vs_2024["kpi"] == kpi) & (vs_2024["quarter"] == quarter)]
            forecast_sum = float(forecast_subset["forecast_value"].sum(skipna=True))
            ref_non_null = int(vs_subset["actual_value_2024"].notna().sum()) if not vs_subset.empty else 0
            ref_sum = float(vs_subset["actual_value_2024"].sum(skipna=True)) if not vs_subset.empty else 0.0
            rows.append(
                {
                    "frequency": label,
                    "kpi": kpi,
                    "quarter": quarter,
                    "forecast_rows": len(forecast_subset),
                    "forecast_sum": forecast_sum,
                    "vs_2024_rows": len(vs_subset),
                    "vs_2024_non_null_rows": ref_non_null,
                    "vs_2024_sum": ref_sum,
                }
            )
            status = "OK" if ref_non_null > 0 else "ERROR"
            audit.add(
                status,
                f"{label}_q{quarter}_{kpi}_forecast_has_reference",
                (
                    f"forecast_rows={len(forecast_subset):,}, forecast_sum={forecast_sum:,.0f}, "
                    f"vs_2024_rows={len(vs_subset):,}, non_null_2024={ref_non_null:,}, sum_2024={ref_sum:,.0f}"
                ),
                kpi=kpi,
                quarter=quarter,
                forecast_rows=len(forecast_subset),
                forecast_sum=forecast_sum,
                vs_2024_rows=len(vs_subset),
                vs_2024_non_null_rows=ref_non_null,
                vs_2024_sum=ref_sum,
            )

    if rows:
        path = DEBUG_DIR / f"picking_2024_{label}_forecast_reference_alignment.csv"
        pd.DataFrame(rows).to_csv(path, index=False)
        audit.add("OK", f"{label}_alignment_saved", f"Saved forecast/reference alignment to {path}", path=str(path))


def load_dashboard_json(table_name: str, audit: AuditLog) -> pd.DataFrame | None:
    path = ROOT / "dashboard" / "public" / "data" / f"{table_name}.json"
    if not path.exists():
        audit.add("WARNING", f"dashboard_{table_name}", f"Missing {path}", path=str(path))
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        df = pd.DataFrame(data)
        audit.add("OK", f"dashboard_{table_name}", f"Loaded dashboard json with {len(df):,} rows", path=str(path), rows=len(df))
        return df
    except Exception as exc:  # pragma: no cover - diagnostic script
        audit.add("ERROR", f"dashboard_{table_name}", f"Could not read dashboard json: {exc}", path=str(path))
        return None


def inspect_dashboard_public_data(audit: AuditLog) -> None:
    audit.section("dashboard public data")
    forecast_diario = load_dashboard_json("consumo_forecast_diario", audit)
    vs_diario = load_dashboard_json("consumo_vs_2024_diario", audit)
    if forecast_diario is not None and vs_diario is not None:
        inspect_forecast_reference_alignment(forecast_diario, vs_diario, audit, "dashboard_daily", "fecha")


def inspect_frontend_contract(audit: AuditLog) -> None:
    audit.section("frontend read-only contract")
    selector_path = ROOT / "dashboard" / "src" / "features" / "forecast" / "utils" / "forecastSelectors.ts"
    card_path = ROOT / "dashboard" / "src" / "features" / "forecast" / "components" / "KpiSummaryCard.tsx"
    try:
        selector_text = selector_path.read_text(encoding="utf-8")
        card_text = card_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        audit.add("WARNING", "frontend_contract", f"Could not read frontend file: {exc}")
        return

    uses_2024 = "actual_value_2024" in selector_text and "value2024" in selector_text
    zero_is_not_missing = "value === null || Number.isNaN(value)" in card_text
    audit.add(
        "OK" if uses_2024 else "WARNING",
        "frontend_value2024_mapping",
        "forecastSelectors maps actual_value_2024 into value2024" if uses_2024 else "Could not confirm value2024 mapping",
        path=str(selector_path),
    )
    audit.add(
        "OK" if zero_is_not_missing else "WARNING",
        "frontend_zero_handling",
        "KpiSummaryCard treats null/NaN as missing, so numeric 0 would render as 0",
        path=str(card_path),
    )


def main() -> None:
    audit = AuditLog()
    audit.lines.append("Picking PI 2024 diagnostic audit")
    audit.lines.append(f"Repo: {ROOT}")

    inspect_source_file(audit)

    movimientos_clean = load_table(DATA_DIR / "interim" / "movimientos_clean", audit, "movimientos_clean")
    fact_picking = load_table(DATA_DIR / "processed" / "fact_picking_dia", audit, "fact_picking_dia")
    consumo_forecast_diario = load_table(OUTPUTS_DIR / "consumption" / "consumo_forecast_diario", audit, "consumo_forecast_diario")
    consumo_forecast_semanal = load_table(OUTPUTS_DIR / "consumption" / "consumo_forecast_semanal", audit, "consumo_forecast_semanal")
    consumo_vs_diario = load_table(OUTPUTS_DIR / "consumption" / "consumo_vs_2024_diario", audit, "consumo_vs_2024_diario")
    consumo_vs_semanal = load_table(OUTPUTS_DIR / "consumption" / "consumo_vs_2024_semanal", audit, "consumo_vs_2024_semanal")

    inspect_movimientos_clean(movimientos_clean, audit)
    inspect_fact_picking(fact_picking, audit)
    inspect_reconstructed_actuals(fact_picking, audit)
    inspect_consumption_table(consumo_vs_diario, audit, "consumo_vs_2024_diario", "fecha")
    inspect_consumption_table(consumo_vs_semanal, audit, "consumo_vs_2024_semanal", "week_start_date")
    inspect_forecast_reference_alignment(consumo_forecast_diario, consumo_vs_diario, audit, "consumption_daily", "fecha")
    inspect_forecast_reference_alignment(consumo_forecast_semanal, consumo_vs_semanal, audit, "consumption_weekly", "week_start_date")
    inspect_dashboard_public_data(audit)
    inspect_frontend_contract(audit)

    audit.write()


if __name__ == "__main__":
    main()
