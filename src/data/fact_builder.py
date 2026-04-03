from __future__ import annotations

import numpy as np
import pandas as pd

from src.data.service_date_utils import build_request_level_service_dates, get_service_target_date_column
from src.utils.date_utils import monday_of_week


SERVICE_COUNT_COLUMNS = [
    "n_entregas_SGE_dia",
    "n_entregas_SGP_dia",
    "n_recogidas_EGE_dia",
    "n_entregas_total_dia",
    "n_recogidas_total_dia",
    "n_servicios_total_dia",
]


def _service_switch_date(settings: dict, albaranes_fact: pd.DataFrame) -> pd.Timestamp:
    configured = settings.get("service_layer", {}).get("switch_date")
    if configured:
        return pd.Timestamp(configured).normalize()
    fallback_days = int(settings.get("service_layer", {}).get("fallback_days_after_albaranes", 1))
    last_albaranes_date = pd.to_datetime(albaranes_fact["fecha"]).max()
    if pd.isna(last_albaranes_date):
        raise ValueError("Cannot infer service switch date without albaranes history or explicit config.service_layer.switch_date")
    return pd.Timestamp(last_albaranes_date).normalize() + pd.Timedelta(days=fallback_days)


def _build_solicitudes_request_level(solicitudes_maestro: pd.DataFrame) -> pd.DataFrame:
    df = solicitudes_maestro.copy()
    target_col = get_service_target_date_column(df, "fecha_inicio_evento")
    df = df[df[target_col].notna()].copy()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "codigo_generico",
                "fecha_creacion",
                "fecha_inicio_evento",
                "fecha_fin_evento",
                "fecha_servicio_objetiva",
                "tipo_servicio",
                "clase_servicio",
                "urgencia_norm",
                "campo_fecha_objetiva_usado",
                "flag_fecha_objetiva_fallback",
                "flag_fecha_objetiva_missing",
                "lineas_solicitadas",
                "articulos_distintos",
                "unidades_solicitadas",
                "kg_solicitados",
                "m3_solicitados",
                "lead_time_dias",
            ]
        )
    if "urgencia_norm" not in df.columns:
        df["urgencia_norm"] = "UNKNOWN"

    df["cantidad_kg"] = df["cant_solicitada"].fillna(0) * df["kilos"].fillna(0)
    df["cantidad_m3"] = df["cant_solicitada"].fillna(0) * df["m3"].fillna(0)

    date_selection = build_request_level_service_dates(df)
    aggregated = (
        df.groupby("codigo_generico")
        .agg(
            fecha_creacion=("fecha_creacion", "min"),
            lineas_solicitadas=("linea_solicitada", "sum"),
            articulos_distintos=("codigo_articulo", pd.Series.nunique),
            unidades_solicitadas=("cant_solicitada", "sum"),
            kg_solicitados=("cantidad_kg", "sum"),
            m3_solicitados=("cantidad_m3", "sum"),
        )
        .reset_index()
    )
    request_level = aggregated.merge(date_selection, on="codigo_generico", how="left")
    request_level["fecha_servicio_objetiva"] = pd.to_datetime(request_level["fecha_servicio_objetiva"]).dt.normalize()
    request_level["lead_time_dias"] = (
        request_level["fecha_servicio_objetiva"] - pd.to_datetime(request_level["fecha_creacion"]).dt.normalize()
    ).dt.days
    return request_level

def build_fact_servicio_dia_from_albaranes(albaranes: pd.DataFrame) -> pd.DataFrame:
    df = albaranes.copy()
    target_col = get_service_target_date_column(df, "fecha_servicio")
    df = df[df[target_col].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=["fecha", *SERVICE_COUNT_COLUMNS, "service_source", "service_truth_status", "is_final_service_truth"])
    df["fecha"] = pd.to_datetime(df[target_col]).dt.normalize()
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

    for source, target in [
        ("peso_kg", "sum_peso_kg_dia"),
        ("vol_m3", "sum_vol_m3_dia"),
        ("kg_volumetrico", "sum_kg_volumetrico_dia"),
        ("kg_facturable", "sum_kg_facturable_dia"),
        ("cajas_in", "sum_cajas_in_dia"),
        ("pales_in", "sum_pales_in_dia"),
        ("m3_in", "sum_m3_in_dia"),
        ("cajas_out", "sum_cajas_out_dia"),
        ("pales_out", "sum_pales_out_dia"),
        ("m3_out", "sum_m3_out_dia"),
    ]:
        out[target] = grouped[source].sum(min_count=1)
        out[f"pct_missing_{source}_dia"] = grouped[source].apply(lambda x: float(x.isna().mean()))

    out["mean_kg_facturable_dia"] = grouped["kg_facturable"].mean()
    out["p50_kg_facturable_dia"] = grouped["kg_facturable"].median()
    out["p90_kg_facturable_dia"] = grouped["kg_facturable"].quantile(0.90)
    out["p95_kg_facturable_dia"] = grouped["kg_facturable"].quantile(0.95)
    out = out.reset_index()
    out["service_source"] = "albaranes"
    out["service_truth_status"] = "observado_final"
    out["is_final_service_truth"] = 1
    return out


def build_fact_servicio_dia_from_solicitudes(solicitudes_maestro: pd.DataFrame) -> pd.DataFrame:
    request_level = _build_solicitudes_request_level(solicitudes_maestro)
    if request_level.empty:
        return pd.DataFrame(columns=["fecha", *SERVICE_COUNT_COLUMNS, "service_source", "service_truth_status", "is_final_service_truth"])
    request_level = request_level.copy()
    request_level["fecha"] = pd.to_datetime(request_level["fecha_servicio_objetiva"]).dt.normalize()
    request_level["is_entrega_sge"] = ((request_level["tipo_servicio"] == "SGE") & (request_level["clase_servicio"] == "entrega")).astype(int)
    request_level["is_entrega_sgp"] = ((request_level["tipo_servicio"] == "SGP") & (request_level["clase_servicio"] == "entrega")).astype(int)
    request_level["is_recogida_ege"] = ((request_level["tipo_servicio"] == "EGE") & (request_level["clase_servicio"] == "recogida")).astype(int)
    request_level["is_entrega"] = (request_level["clase_servicio"] == "entrega").astype(int)
    request_level["is_recogida"] = (request_level["clase_servicio"] == "recogida").astype(int)
    for urgency in ["NO", "SI", "MUY_URGENTE", "UNKNOWN"]:
        request_level[f"is_urgencia_{urgency.lower()}"] = (request_level["urgencia_norm"] == urgency).astype(int)

    grouped = request_level.groupby("fecha")
    out = pd.DataFrame(index=grouped.size().index)
    out["n_entregas_SGE_dia"] = grouped["is_entrega_sge"].sum()
    out["n_entregas_SGP_dia"] = grouped["is_entrega_sgp"].sum()
    out["n_recogidas_EGE_dia"] = grouped["is_recogida_ege"].sum()
    out["n_entregas_total_dia"] = grouped["is_entrega"].sum()
    out["n_recogidas_total_dia"] = grouped["is_recogida"].sum()
    out["n_servicios_total_dia"] = grouped.size().astype(int)
    for urgency in ["NO", "SI", "MUY_URGENTE", "UNKNOWN"]:
        out[f"n_urgencia_{urgency.lower()}_dia"] = grouped[f"is_urgencia_{urgency.lower()}"].sum()
    out["lineas_solicitadas_dia"] = grouped["lineas_solicitadas"].sum()
    out["articulos_distintos_dia"] = grouped["articulos_distintos"].sum()
    out["unidades_solicitadas_dia"] = grouped["unidades_solicitadas"].sum()
    out["kg_solicitados_dia"] = grouped["kg_solicitados"].sum()
    out["m3_solicitados_dia"] = grouped["m3_solicitados"].sum()
    out = out.reset_index()
    out["service_source"] = "solicitudes"
    out["service_truth_status"] = "visible_cartera"
    out["is_final_service_truth"] = 0
    return out


def build_fact_servicio_dia(
    albaranes: pd.DataFrame,
    solicitudes_maestro: pd.DataFrame,
    settings: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    albaranes_fact = build_fact_servicio_dia_from_albaranes(albaranes)
    solicitudes_fact = build_fact_servicio_dia_from_solicitudes(solicitudes_maestro)
    switch_date = _service_switch_date(settings, albaranes_fact)

    common_columns = sorted(set(albaranes_fact.columns).union(solicitudes_fact.columns))
    albaranes_fact = albaranes_fact.reindex(columns=common_columns)
    solicitudes_fact = solicitudes_fact.reindex(columns=common_columns)

    hybrid = pd.concat(
        [
            albaranes_fact[pd.to_datetime(albaranes_fact["fecha"]) < switch_date],
            solicitudes_fact[pd.to_datetime(solicitudes_fact["fecha"]) >= switch_date],
        ],
        ignore_index=True,
        sort=False,
    ).sort_values("fecha")

    hybrid = hybrid.drop_duplicates(subset=["fecha"], keep="last").reset_index(drop=True)
    overlap_days = sorted(
        set(pd.to_datetime(albaranes_fact.loc[pd.to_datetime(albaranes_fact["fecha"]) >= switch_date, "fecha"]).dt.date)
        .intersection(pd.to_datetime(solicitudes_fact.loc[pd.to_datetime(solicitudes_fact["fecha"]) >= switch_date, "fecha"]).dt.date)
    )
    source_day_counts = hybrid["service_source"].value_counts().to_dict()
    audit_rows = [
        {"metric": "service_switch_date", "value": str(switch_date.date())},
        {"metric": "last_albaranes_date", "value": str(pd.to_datetime(albaranes_fact["fecha"]).max().date()) if not albaranes_fact.empty else ""},
        {"metric": "last_solicitudes_visible_date", "value": str(pd.to_datetime(solicitudes_fact["fecha"]).max().date()) if not solicitudes_fact.empty else ""},
        {"metric": "last_hybrid_date", "value": str(pd.to_datetime(hybrid["fecha"]).max().date()) if not hybrid.empty else ""},
        {"metric": "days_from_albaranes", "value": int(source_day_counts.get("albaranes", 0))},
        {"metric": "days_from_solicitudes", "value": int(source_day_counts.get("solicitudes", 0))},
        {"metric": "overlap_days_ge_switch", "value": len(overlap_days)},
        {"metric": "unknown_service_rows_solicitudes", "value": int(solicitudes_maestro["tipo_servicio"].eq("UNK").sum())},
        {"metric": "unknown_service_pct_solicitudes", "value": float(solicitudes_maestro["tipo_servicio"].eq("UNK").mean()) if len(solicitudes_maestro) else 0.0},
        {"metric": "solicitudes_rows_sge", "value": int(solicitudes_maestro["tipo_servicio"].eq("SGE").sum())},
        {"metric": "solicitudes_rows_sgp", "value": int(solicitudes_maestro["tipo_servicio"].eq("SGP").sum())},
        {"metric": "solicitudes_rows_ege", "value": int(solicitudes_maestro["tipo_servicio"].eq("EGE").sum())},
    ]
    audit_report = pd.DataFrame(audit_rows)
    audit_meta = {
        "switch_date": switch_date,
        "last_albaranes_date": pd.to_datetime(albaranes_fact["fecha"]).max() if not albaranes_fact.empty else pd.NaT,
        "last_solicitudes_visible_date": pd.to_datetime(solicitudes_fact["fecha"]).max() if not solicitudes_fact.empty else pd.NaT,
        "last_hybrid_date": pd.to_datetime(hybrid["fecha"]).max() if not hybrid.empty else pd.NaT,
        "days_from_albaranes": int(source_day_counts.get("albaranes", 0)),
        "days_from_solicitudes": int(source_day_counts.get("solicitudes", 0)),
        "overlap_days_ge_switch": overlap_days,
    }
    return hybrid, audit_report, audit_meta


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
    request_level = _build_solicitudes_request_level(solicitudes_maestro)
    target_col = get_service_target_date_column(request_level, "fecha_inicio_evento")
    request_level["lead_time_dias"] = (pd.to_datetime(request_level[target_col]) - pd.to_datetime(request_level["fecha_creacion"]).dt.normalize()).dt.days
    by_target_date = (
        request_level.groupby([target_col, "tipo_servicio", "clase_servicio"]).agg(
            pedidos_abiertos=("codigo_generico", "nunique"),
            lineas_solicitadas=("lineas_solicitadas", "sum"),
            articulos_distintos=("articulos_distintos", "sum"),
            unidades_solicitadas=("unidades_solicitadas", "sum"),
            kg_solicitados=("kg_solicitados", "sum"),
            m3_solicitados=("m3_solicitados", "sum"),
        )
        .reset_index()
        .rename(columns={target_col: "fecha_objetivo"})
    )

    horizons = []
    valid = request_level.dropna(subset=["fecha_creacion", target_col]).copy()
    for horizon in range(1, 29):
        snapshot = valid.copy()
        snapshot["fecha_snapshot"] = snapshot[target_col] - pd.to_timedelta(horizon, unit="D")
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
