from __future__ import annotations

import pandas as pd

from src.features.cartera_features import build_cartera_snapshot


def fit_cartera_scalers(actual_service_fact: pd.DataFrame, fact_cartera: pd.DataFrame) -> pd.DataFrame:
    actual = actual_service_fact[["fecha", "n_entregas_SGE_dia", "n_entregas_SGP_dia", "n_recogidas_EGE_dia"]].copy()
    actual = actual.melt(id_vars="fecha", var_name="kpi", value_name="actual")
    actual["tipo_servicio"] = actual["kpi"].map({
        "n_entregas_SGE_dia": "SGE",
        "n_entregas_SGP_dia": "SGP",
        "n_recogidas_EGE_dia": "EGE",
    })
    cartera = fact_cartera.groupby(["fecha_inicio_evento", "tipo_servicio"])["codigo_generico"].nunique().reset_index(name="pedidos_abiertos")
    merged = actual.merge(cartera, left_on=["fecha", "tipo_servicio"], right_on=["fecha_inicio_evento", "tipo_servicio"], how="left")
    merged["pedidos_abiertos"] = merged["pedidos_abiertos"].replace(0, pd.NA)
    merged["ratio"] = merged["actual"] / merged["pedidos_abiertos"]
    return merged.groupby("tipo_servicio")["ratio"].median().fillna(1.0).reset_index(name="scaler")


def apply_nowcasting(base_forecast: pd.DataFrame, fact_cartera: pd.DataFrame, scalers: pd.DataFrame, origin_date: pd.Timestamp, settings: dict) -> pd.DataFrame:
    snapshot = build_cartera_snapshot(fact_cartera, origin_date)
    if snapshot.empty:
        return base_forecast
    snapshot = snapshot.merge(scalers, on="tipo_servicio", how="left")
    snapshot["cartera_signal"] = snapshot["pedidos_abiertos"] * snapshot["scaler"].fillna(1.0)
    adjusted = base_forecast.merge(snapshot[["fecha_objetivo", "tipo_servicio", "cartera_signal"]], left_on=["fecha", "tipo_servicio"], right_on=["fecha_objetivo", "tipo_servicio"], how="left")
    adjusted["cartera_signal"] = adjusted["cartera_signal"].fillna(adjusted["forecast"])
    horizon = (pd.to_datetime(adjusted["fecha"]) - pd.Timestamp(origin_date).normalize()).dt.days
    short_mask = horizon.between(0, settings["nowcasting"]["short_horizon_max"])
    medium_mask = horizon.between(settings["nowcasting"]["short_horizon_max"] + 1, settings["nowcasting"]["medium_horizon_max"])
    adjusted.loc[short_mask, "forecast"] = adjusted.loc[short_mask, "forecast"] * settings["nowcasting"]["statistical_weight_short"] + adjusted.loc[short_mask, "cartera_signal"] * settings["nowcasting"]["cartera_weight_short"]
    adjusted.loc[medium_mask, "forecast"] = adjusted.loc[medium_mask, "forecast"] * settings["nowcasting"]["statistical_weight_medium"] + adjusted.loc[medium_mask, "cartera_signal"] * settings["nowcasting"]["cartera_weight_medium"]
    return adjusted.drop(columns=["fecha_objetivo", "cartera_signal"], errors="ignore")
