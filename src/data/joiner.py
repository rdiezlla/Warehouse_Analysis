from __future__ import annotations

import logging
from typing import Any

import pandas as pd

LOGGER = logging.getLogger(__name__)


def _coverage_summary(df: pd.DataFrame, matched_col: str, group_col: str | None = None) -> pd.DataFrame:
    if group_col and group_col in df.columns:
        grouped = df.groupby(group_col)[matched_col].agg(rows="size", matched="sum")
        grouped["coverage_pct"] = grouped["matched"] / grouped["rows"]
        grouped = grouped.reset_index()
        return grouped
    coverage = pd.DataFrame({"rows": [len(df)], "matched": [int(df[matched_col].sum())]})
    coverage["coverage_pct"] = coverage["matched"] / coverage["rows"].clip(lower=1)
    return coverage


def build_join_outputs(albaranes: pd.DataFrame, movimientos: pd.DataFrame, solicitudes: pd.DataFrame, maestro_dedup: pd.DataFrame) -> dict[str, Any]:
    albaranes_join_columns = [
        "codigo_servicio",
        "fecha_servicio",
        "fecha_servicio_objetiva",
        "campo_fecha_objetiva_usado",
        "tipo_servicio",
        "clase_servicio",
        "urgencia_norm",
    ]
    mov_alb = movimientos.merge(
        albaranes[albaranes_join_columns].drop_duplicates("codigo_servicio"),
        left_on="pedido_externo",
        right_on="codigo_servicio",
        how="left",
        suffixes=("", "_albaran"),
    )
    mov_alb["match_albaranes"] = mov_alb["codigo_servicio"].notna()

    sol_alb = solicitudes.merge(
        albaranes[albaranes_join_columns].drop_duplicates("codigo_servicio"),
        left_on="codigo_generico",
        right_on="codigo_servicio",
        how="left",
        suffixes=("", "_albaran"),
    )
    sol_alb["match_albaranes"] = sol_alb["codigo_servicio"].notna()

    mov_mae = movimientos.merge(maestro_dedup, left_on="codigo_articulo", right_on="codigo", how="left", suffixes=("", "_maestro"))
    mov_mae["match_maestro"] = mov_mae["codigo"].notna()

    sol_mae = solicitudes.merge(maestro_dedup, left_on="codigo_articulo", right_on="codigo", how="left", suffixes=("", "_maestro"))
    sol_mae["match_maestro"] = sol_mae["codigo"].notna()

    coverage_rows = [
        {"join_name": "movimientos_albaranes_global", **_coverage_summary(mov_alb, "match_albaranes").iloc[0].to_dict()},
        {"join_name": "solicitudes_albaranes_global", **_coverage_summary(sol_alb, "match_albaranes").iloc[0].to_dict()},
        {"join_name": "movimientos_maestro_global", **_coverage_summary(mov_mae, "match_maestro").iloc[0].to_dict()},
        {"join_name": "solicitudes_maestro_global", **_coverage_summary(sol_mae, "match_maestro").iloc[0].to_dict()},
    ]
    coverage = pd.DataFrame(coverage_rows)

    coverage_by_service = _coverage_summary(mov_alb, "match_albaranes", "tipo_servicio")
    coverage_by_movement = _coverage_summary(mov_alb, "match_albaranes", "tipo_movimiento")
    unmatched = pd.DataFrame(
        {
            "movimientos_no_match_top": mov_alb.loc[~mov_alb["match_albaranes"], "pedido_externo"].fillna("<NULL>").value_counts().head(50),
            "solicitudes_no_match_top": sol_alb.loc[~sol_alb["match_albaranes"], "codigo_generico"].fillna("<NULL>").value_counts().head(50),
        }
    ).fillna(0).reset_index().rename(columns={"index": "value"})
    internal_movements = mov_alb.loc[~mov_alb["match_albaranes"], "pedido_externo"].dropna().astype(str)
    internal_summary = (
        internal_movements.str.extract(r"^([A-Z]+)")[0].fillna("OTHER").value_counts().head(50).reset_index().rename(columns={"index": "prefix", 0: "count"})
    )

    return {
        "movimientos_albaranes": mov_alb,
        "solicitudes_albaranes": sol_alb,
        "movimientos_maestro": mov_mae,
        "solicitudes_maestro": sol_mae,
        "coverage_global": coverage,
        "coverage_by_service": coverage_by_service,
        "coverage_by_movement": coverage_by_movement,
        "unmatched_top": unmatched,
        "internal_movement_summary": internal_summary,
    }
