from __future__ import annotations

import pandas as pd


def build_cartera_snapshot(fact_cartera: pd.DataFrame, origin_date: pd.Timestamp) -> pd.DataFrame:
    origin_date = pd.Timestamp(origin_date).normalize()
    snapshot = fact_cartera[(fact_cartera["fecha_creacion"] <= origin_date) & (fact_cartera["fecha_inicio_evento"] >= origin_date)].copy()
    if snapshot.empty:
        return pd.DataFrame(columns=["fecha_objetivo", "tipo_servicio", "pedidos_abiertos", "lineas_solicitadas", "unidades_solicitadas"])
    return (
        snapshot.groupby(["fecha_inicio_evento", "tipo_servicio"]).agg(
            pedidos_abiertos=("codigo_generico", "nunique"),
            lineas_solicitadas=("lineas_solicitadas", "sum"),
            unidades_solicitadas=("unidades_solicitadas", "sum"),
        )
        .reset_index()
        .rename(columns={"fecha_inicio_evento": "fecha_objetivo"})
    )
