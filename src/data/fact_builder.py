from __future__ import annotations

import numpy as np
import pandas as pd

from src.utils.date_utils import monday_of_week


def build_fact_servicio_dia(albaranes: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    df = albaranes.copy()
    df = df[df["fecha_servicio"].notna()].copy()
    df["fecha"] = df["fecha_servicio"].dt.normalize()
    df["is_entrega_sge"] = ((df["tipo_servicio"] == "SGE") & (df["clase_servicio"] == "entrega")).astype(int)
    df["is_entrega_sgp"] = ((df["tipo_servicio"] == "SGP") & (df["clase_servicio"] == "entrega")).astype(int)
    df["is_recogida_ege"] = ((df["tipo_servicio"] == "EGE") & (df["clase_servicio"] == "recogida")).astype(int)
    df["is_entrega"] = (df["clase_servicio"] == "entrega").astype(int)
    df["is_recogida"] = (df["clase_servicio"] == "recogida").astype(int)
    for urgency in ["NO", "SI", "MUY_URGENTE", "UNKNOWN"]:
        df[f"is_urgencia_{urgency.lower()}"] = (df["urgencia_norm"] == urgency).astype(int)
    grouped = df.groupby("fecha")

    out = pd.DataFrame(index=grouped.size().index)
    out["n_entregas_SGE_dia"] = grouped["is_entrega_sge"].sum()
    out["n_entregas_SGP_dia"] = grouped["is_entrega_sgp"].sum()
    out["n_recogidas_EGE_dia"] = grouped["is_recogida_ege"].sum()
    out["n_entregas_total_dia"] = grouped["is_entrega"].sum()
    out["n_recogidas_total_dia"] = grouped["is_recogida"].sum()
    out["n_servicios_total_dia"] = grouped.size().astype(int)

    for urgency in ["NO", "SI", "MUY_URGENTE", "UNKNOWN"]:
        out[f"n_urgencia_{urgency.lower()}_dia"] = grouped[f"is_urgencia_{urgency.lower()}"].sum()

    for source, target in [("peso_kg", "sum_peso_kg_dia"), ("vol_m3", "sum_vol_m3_dia"), ("kg_volumetrico", "sum_kg_volumetrico_dia"), ("kg_facturable", "sum_kg_facturable_dia"), ("cajas_in", "sum_cajas_in_dia"), ("pales_in", "sum_pales_in_dia"), ("m3_in", "sum_m3_in_dia"), ("cajas_out", "sum_cajas_out_dia"), ("pales_out", "sum_pales_out_dia"), ("m3_out", "sum_m3_out_dia")]:
        out[target] = grouped[source].sum(min_count=1)
        out[f"pct_missing_{source}_dia"] = grouped[source].apply(lambda x: float(x.isna().mean()))

    out["mean_kg_facturable_dia"] = grouped["kg_facturable"].mean()
    out["p50_kg_facturable_dia"] = grouped["kg_facturable"].median()
    out["p90_kg_facturable_dia"] = grouped["kg_facturable"].quantile(0.90)
    out["p95_kg_facturable_dia"] = grouped["kg_facturable"].quantile(0.95)
    return out.reset_index()


def build_fact_picking_dia(movimientos_maestro: pd.DataFrame, heavy_item_kg: float, bulky_item_m3: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = movimientos_maestro.copy()
    df = df[df["fecha_operativa_mov"].notna()].copy()
    df["cantidad"] = df["cantidad"].fillna(0)
    df["kilos"] = df["kilos"].fillna(0)
    df["m3"] = df["m3"].fillna(0)
    df["peso_estimado_mov"] = df["cantidad"] * df["kilos"]
    df["vol_estimado_mov"] = df["cantidad"] * df["m3"]
    df["flag_item_pesado"] = (df["kilos"] >= heavy_item_kg).astype(int)
    df["flag_item_voluminoso"] = (df["m3"] >= bulky_item_m3).astype(int)

    pi = df[df["tipo_movimiento"] == "PI"].copy()
    grouped = pi.groupby("fecha_operativa_mov")
    fact = pd.DataFrame(index=grouped.size().index)
    fact["picking_lines_PI_dia"] = grouped.size().astype(int)
    fact["picking_units_PI_dia"] = grouped["cantidad"].sum()
    fact["picking_articulos_distintos_PI_dia"] = grouped["codigo_articulo"].nunique()
    fact["picking_kg_dia"] = grouped["peso_estimado_mov"].sum()
    fact["picking_m3_dia"] = grouped["vol_estimado_mov"].sum()
    fact["pct_items_pesados_dia"] = grouped["flag_item_pesado"].mean()
    fact["pct_items_voluminosos_dia"] = grouped["flag_item_voluminoso"].mean()
    fact["maestro_match_rate_dia"] = grouped["match_maestro"].mean()
    fact = fact.reset_index().rename(columns={"fecha_operativa_mov": "fecha"})

    prepared = df[df["tipo_movimiento"].isin(["CR", "EP"])].groupby(["fecha_operativa_mov", "tipo_movimiento"]).size().unstack(fill_value=0).reset_index()
    prepared = prepared.rename(columns={"fecha_operativa_mov": "fecha", "CR": "entradas_CR_dia", "EP": "ubicaciones_EP_dia"})
    return fact, prepared


def build_fact_cartera(solicitudes_maestro: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = solicitudes_maestro.copy()
    df["cantidad_kg"] = df["cant_solicitada"].fillna(0) * df["kilos"].fillna(0)
    df["cantidad_m3"] = df["cant_solicitada"].fillna(0) * df["m3"].fillna(0)
    request_level = (
        df.groupby("codigo_generico")
        .agg(
            fecha_creacion=("fecha_creacion", "min"),
            fecha_inicio_evento=("fecha_inicio_evento", "min"),
            tipo_servicio=("tipo_servicio", lambda x: x.dropna().iloc[0] if x.dropna().any() else "UNK"),
            clase_servicio=("clase_servicio", lambda x: x.dropna().iloc[0] if x.dropna().any() else "UNK"),
            lineas_solicitadas=("linea_solicitada", "sum"),
            articulos_distintos=("codigo_articulo", pd.Series.nunique),
            unidades_solicitadas=("cant_solicitada", "sum"),
            kg_solicitados=("cantidad_kg", "sum"),
            m3_solicitados=("cantidad_m3", "sum"),
        )
        .reset_index()
    )
    request_level["lead_time_dias"] = (request_level["fecha_inicio_evento"] - request_level["fecha_creacion"]).dt.days
    by_target_date = (
        request_level.groupby(["fecha_inicio_evento", "tipo_servicio", "clase_servicio"]).agg(
            pedidos_abiertos=("codigo_generico", "nunique"),
            lineas_solicitadas=("lineas_solicitadas", "sum"),
            articulos_distintos=("articulos_distintos", "sum"),
            unidades_solicitadas=("unidades_solicitadas", "sum"),
            kg_solicitados=("kg_solicitados", "sum"),
            m3_solicitados=("m3_solicitados", "sum"),
        )
        .reset_index()
        .rename(columns={"fecha_inicio_evento": "fecha_objetivo"})
    )

    horizons = []
    valid = request_level.dropna(subset=["fecha_creacion", "fecha_inicio_evento"]).copy()
    for horizon in range(1, 29):
        snapshot = valid.copy()
        snapshot["fecha_snapshot"] = snapshot["fecha_inicio_evento"] - pd.to_timedelta(horizon, unit="D")
        snapshot["horizonte_dias"] = horizon
        horizons.append(snapshot[["fecha_snapshot", "horizonte_dias", "tipo_servicio", "codigo_generico"]])
    by_horizon = (
        pd.concat(horizons, ignore_index=True)
        .groupby(["fecha_snapshot", "horizonte_dias", "tipo_servicio"])['codigo_generico']
        .nunique()
        .reset_index(name="stock_pedidos_abiertos")
    )
    return request_level, by_target_date, by_horizon


def build_weekly_fact(df: pd.DataFrame, date_col: str = "fecha") -> pd.DataFrame:
    weekly = df.copy()
    weekly[date_col] = pd.to_datetime(weekly[date_col])
    weekly["fecha_semana"] = monday_of_week(weekly[date_col])
    numeric_cols = [
        column
        for column in weekly.columns
        if column not in {date_col, "fecha_semana"} and pd.api.types.is_numeric_dtype(weekly[column])
    ]
    out = weekly.groupby("fecha_semana")[numeric_cols].sum().reset_index().rename(columns={"fecha_semana": "fecha"})
    return out
