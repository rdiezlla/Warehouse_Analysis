from __future__ import annotations

import pandas as pd


def classify_abc(movimientos: pd.DataFrame, stock: pd.DataFrame | None = None) -> pd.DataFrame:
    if movimientos.empty:
        return pd.DataFrame(
            columns=[
                "rank",
                "sku",
                "sku_description",
                "lines",
                "units",
                "stock_quantity",
                "percentage",
                "cumulative_percentage",
                "abc_category",
                "last_pi_date",
                "cr_lines",
                "last_cr_date",
            ]
        )

    df = movimientos.copy()
    df["movement_type"] = df["movement_type"].astype("string").str.upper()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    pi = df[df["movement_type"].eq("PI") & df["sku"].notna()].copy()
    cr = df[df["movement_type"].eq("CR") & df["sku"].notna()].copy()

    grouped = (
        pi.groupby("sku", dropna=False)
        .agg(
            sku_description=("sku_description", "first"),
            lines=("sku", "size"),
            units=("quantity", "sum"),
            last_pi_date=("operational_date", "max"),
        )
        .reset_index()
    )

    if grouped.empty:
        return grouped

    cr_summary = (
        cr.groupby("sku", dropna=False)
        .agg(cr_lines=("sku", "size"), last_cr_date=("operational_date", "max"))
        .reset_index()
    )
    grouped = grouped.merge(cr_summary, on="sku", how="left")
    grouped["cr_lines"] = grouped["cr_lines"].fillna(0).astype(int)

    if stock is not None and not stock.empty:
        stock_summary = (
            stock.groupby("sku", dropna=False)
            .agg(stock_quantity=("stock_quantity", "sum"))
            .reset_index()
        )
        grouped = grouped.merge(stock_summary, on="sku", how="left")
    else:
        grouped["stock_quantity"] = pd.NA

    grouped = grouped.sort_values(["lines", "units", "sku"], ascending=[False, False, True]).reset_index(drop=True)
    grouped["rank"] = grouped.index + 1
    total_lines = float(grouped["lines"].sum())
    grouped["percentage"] = grouped["lines"] / total_lines if total_lines else 0.0
    grouped["cumulative_percentage"] = grouped["percentage"].cumsum()
    cumulative_before = grouped["cumulative_percentage"] - grouped["percentage"]
    grouped["abc_category"] = "C"
    grouped.loc[cumulative_before < 0.80, "abc_category"] = "A"
    grouped.loc[(cumulative_before >= 0.80) & (cumulative_before < 0.95), "abc_category"] = "B"
    grouped["stock_coverage_lines"] = grouped["stock_quantity"] / grouped["lines"].where(grouped["lines"] > 0)
    grouped["relocation_recommendation"] = "Mantener en almacen principal"
    grouped.loc[
        grouped["abc_category"].eq("C") & grouped["stock_quantity"].fillna(0).gt(0),
        "relocation_recommendation",
    ] = "Revisar para almacen secundario"
    grouped.loc[
        grouped["abc_category"].eq("B") & grouped["stock_coverage_lines"].fillna(0).gt(20),
        "relocation_recommendation",
    ] = "Revisar exceso de stock"
    grouped.loc[
        grouped["abc_category"].eq("A"),
        "relocation_recommendation",
    ] = "Mantener accesible"

    return grouped[
        [
            "rank",
            "sku",
            "sku_description",
            "lines",
            "units",
            "stock_quantity",
            "percentage",
            "cumulative_percentage",
            "abc_category",
            "last_pi_date",
            "cr_lines",
            "last_cr_date",
            "stock_coverage_lines",
            "relocation_recommendation",
        ]
    ]
