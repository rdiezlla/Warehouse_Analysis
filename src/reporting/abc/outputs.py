from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.utils.io_utils import ensure_dirs, save_dataframe


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


def _write_audit_workbook(abc_sku: pd.DataFrame, output_dir: Path) -> Path:
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
    return audit_path


def write_abc_outputs(abc_sku: pd.DataFrame, output_dir: Path, dashboard_dir: Path) -> dict[str, Path]:
    ensure_dirs(output_dir, dashboard_dir)
    save_dataframe(abc_sku, output_dir / "abc_sku", index=False)
    audit_path = _write_audit_workbook(abc_sku, output_dir)
    abc_sku.to_json(dashboard_dir / "abc_sku.json", orient="records", force_ascii=False, indent=2, date_format="iso")
    return {
        "csv": output_dir / "abc_sku.csv",
        "parquet": output_dir / "abc_sku.parquet",
        "audit": audit_path,
        "json": dashboard_dir / "abc_sku.json",
    }
