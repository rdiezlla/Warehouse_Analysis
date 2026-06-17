from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.modeling.abc.legacy_outputs import ABCLegacyOutputs
from src.utils.io_utils import ensure_dirs, save_dataframe, save_parquet_safe, write_json


def _build_category_summary(abc_sku: pd.DataFrame) -> pd.DataFrame:
    if abc_sku.empty:
        return pd.DataFrame(columns=["abc_category", "skus", "lines", "units", "stock_quantity"])
    return (
        abc_sku.groupby("abc_category", dropna=False)
        .agg(
            skus=("sku", "nunique"),
            lines=("lines", "sum"),
            units=("units", "sum"),
            stock_quantity=("stock_quantity", "sum"),
        )
        .reset_index()
        .sort_values("abc_category")
    )


def _write_audit_workbook(abc_sku: pd.DataFrame, output_dir: Path, legacy_outputs: ABCLegacyOutputs | None = None) -> Path:
    audit_path = output_dir / "abc_auditoria.xlsx"
    summary = _build_category_summary(abc_sku)
    candidates = (
        abc_sku.loc[
            abc_sku["relocation_recommendation"].ne("Mantener accesible")
            & abc_sku["relocation_recommendation"].ne("Mantener en almacen principal")
        ]
        .sort_values(["abc_category", "stock_quantity", "lines"], ascending=[False, False, True])
        .head(500)
    )
    criteria = pd.DataFrame(
        [
            {
                "criterio": "ABC",
                "detalle": "A hasta 80% acumulado de lineas PI, B hasta 95%, C resto.",
            },
            {
                "criterio": "Revisar para almacen secundario",
                "detalle": "SKU C con stock positivo.",
            },
            {
                "criterio": "Revisar exceso de stock",
                "detalle": "SKU B con stock_coverage_lines superior a 20.",
            },
            {
                "criterio": "Mantener accesible",
                "detalle": "SKU A por alta rotacion.",
            },
        ]
    )
    with pd.ExcelWriter(audit_path, engine="openpyxl") as writer:
        abc_sku.to_excel(writer, sheet_name="Ranking ABC", index=False)
        summary.to_excel(writer, sheet_name="Resumen categoria", index=False)
        candidates.to_excel(writer, sheet_name="Candidatos traslado", index=False)
        criteria.to_excel(writer, sheet_name="Criterios", index=False)
        if legacy_outputs is not None:
            sheet_map = {
                "stock_abc_actual_article": "ABC articulo unico YTD",
                "stock_abc_actual_owner_article": "ABC YTD owner-articulo",
                "stock_abc_30d_article": "ABC articulo unico 30d",
                "stock_abc_30d_owner_article": "ABC 30d owner-articulo",
                "stock_abc_historico_trimestral_article": "ABC trimestral articulo",
                "stock_abc_historico_trimestral_owner_article": "ABC trimestral owner-articulo",
                "stock_abc_cambios_trimestrales": "Cambios ABC trimestral",
                "stock_abc_temporalidad_article": "Temporalidad articulo",
                "stock_abc_temporalidad_monthly_article": "Temporalidad mensual articulo",
                "stock_abc_temporalidad_quarterly_article": "Temporalidad trim articulo",
                "stock_abc_decision_almacen_article": "Decision almacen articulo",
                "stock_abc_resumen_clases": "Resumen legacy",
            }
            for dataset_name, sheet_name in sheet_map.items():
                frame = legacy_outputs.datasets.get(dataset_name)
                if frame is not None and not frame.empty:
                    frame.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    return audit_path


def _write_legacy_outputs(legacy_outputs: ABCLegacyOutputs, output_dir: Path, dashboard_dir: Path) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    csv_dir = output_dir / "csv"
    parquet_dir = output_dir / "parquet"
    json_dir = output_dir / "json"
    ensure_dirs(csv_dir, parquet_dir, json_dir, dashboard_dir)
    for dataset_name, frame in legacy_outputs.datasets.items():
        if frame.empty:
            continue
        csv_path = csv_dir / f"{dataset_name}.csv"
        parquet_path = parquet_dir / f"{dataset_name}.parquet"
        frame.to_csv(csv_path, index=False)
        save_parquet_safe(frame, parquet_path, index=False)
        paths[f"{dataset_name}_csv"] = csv_path
        paths[f"{dataset_name}_parquet"] = parquet_dir / f"{dataset_name}.parquet"

    kpi_path = json_dir / "stock_abc_resumen_kpis.json"
    temporality_path = json_dir / "stock_abc_resumen_temporalidad.json"
    write_json(kpi_path, legacy_outputs.kpis)
    write_json(temporality_path, legacy_outputs.temporality_kpis)
    write_json(dashboard_dir / "stock_abc_resumen_kpis.json", legacy_outputs.kpis)
    write_json(dashboard_dir / "stock_abc_resumen_temporalidad.json", legacy_outputs.temporality_kpis)
    paths["stock_abc_resumen_kpis_json"] = kpi_path
    paths["stock_abc_resumen_temporalidad_json"] = temporality_path
    return paths


def write_abc_outputs(
    abc_sku: pd.DataFrame,
    output_dir: Path,
    dashboard_dir: Path,
    legacy_outputs: ABCLegacyOutputs | None = None,
) -> dict[str, Path]:
    ensure_dirs(output_dir, dashboard_dir)
    save_dataframe(abc_sku, output_dir / "abc_sku", index=False)
    legacy_paths = _write_legacy_outputs(legacy_outputs, output_dir, dashboard_dir) if legacy_outputs is not None else {}
    audit_path = _write_audit_workbook(abc_sku, output_dir, legacy_outputs)
    abc_sku.to_json(dashboard_dir / "abc_sku.json", orient="records", force_ascii=False, indent=2, date_format="iso")
    return {
        "csv": output_dir / "abc_sku.csv",
        "parquet": output_dir / "abc_sku.parquet",
        "audit": audit_path,
        "json": dashboard_dir / "abc_sku.json",
        **legacy_paths,
    }
