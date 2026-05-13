from __future__ import annotations

import pandas as pd

from src.data.service_date_utils import get_service_target_date_column


def build_cartera_snapshot(fact_cartera: pd.DataFrame, origin_date: pd.Timestamp) -> pd.DataFrame:
    origin_date = pd.Timestamp(origin_date).normalize()
    target_col = get_service_target_date_column(fact_cartera, "fecha_inicio_evento")
    snapshot = fact_cartera[(fact_cartera["fecha_creacion"] <= origin_date) & (fact_cartera[target_col] >= origin_date)].copy()
    if snapshot.empty:
        return pd.DataFrame(columns=["fecha_objetivo", "tipo_servicio", "pedidos_abiertos", "lineas_solicitadas", "unidades_solicitadas"])
    return (
        snapshot.groupby([target_col, "tipo_servicio"]).agg(
            pedidos_abiertos=("codigo_generico", "nunique"),
            lineas_solicitadas=("lineas_solicitadas", "sum"),
            unidades_solicitadas=("unidades_solicitadas", "sum"),
        )
        .reset_index()
        .rename(columns={target_col: "fecha_objetivo"})
    )
