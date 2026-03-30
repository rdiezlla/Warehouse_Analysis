from __future__ import annotations

import numpy as np
import pandas as pd

from src.features.cartera_features import build_cartera_snapshot
from src.modeling.calibration import resolve_nowcasting_weights


def build_cartera_maturity_curves(fact_cartera: pd.DataFrame, max_horizon: int = 28) -> pd.DataFrame:
    if fact_cartera.empty:
        return pd.DataFrame(columns=["tipo_servicio", "horizonte_dias", "visible_ratio_pedidos"])
    requests = fact_cartera[["codigo_generico", "fecha_creacion", "fecha_inicio_evento", "tipo_servicio"]].dropna().copy()
    final_counts = requests.groupby(["fecha_inicio_evento", "tipo_servicio"])["codigo_generico"].nunique().reset_index(name="final_pedidos")
    rows = []
    for horizon in range(0, max_horizon + 1):
        visible = requests[requests["fecha_creacion"] <= (requests["fecha_inicio_evento"] - pd.to_timedelta(horizon, unit="D"))]
        visible_counts = visible.groupby(["fecha_inicio_evento", "tipo_servicio"])["codigo_generico"].nunique().reset_index(name="visible_pedidos")
        merged = final_counts.merge(visible_counts, on=["fecha_inicio_evento", "tipo_servicio"], how="left").fillna({"visible_pedidos": 0})
        merged["visible_ratio_pedidos"] = merged["visible_pedidos"] / merged["final_pedidos"].replace(0, np.nan)
        summary = (
            merged.groupby("tipo_servicio")["visible_ratio_pedidos"]
            .median()
            .reset_index()
            .assign(horizonte_dias=horizon)
        )
        rows.append(summary)
    curves = pd.concat(rows, ignore_index=True)
    curves["visible_ratio_pedidos"] = curves["visible_ratio_pedidos"].clip(lower=0.05, upper=1.0).fillna(1.0)
    return curves[["tipo_servicio", "horizonte_dias", "visible_ratio_pedidos"]]


def fit_cartera_scalers(actual_service_fact: pd.DataFrame, fact_cartera: pd.DataFrame) -> pd.DataFrame:
    final_truth = actual_service_fact.copy()
    if "is_final_service_truth" in final_truth.columns:
        final_truth = final_truth[final_truth["is_final_service_truth"] == 1].copy()
    if final_truth.empty:
        final_truth = actual_service_fact.copy()
    actual = final_truth[["fecha", "n_entregas_SGE_dia", "n_entregas_SGP_dia", "n_recogidas_EGE_dia"]].copy()
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


def apply_nowcasting(base_forecast: pd.DataFrame, fact_cartera: pd.DataFrame, scalers: pd.DataFrame, origin_date: pd.Timestamp, settings: dict, maturity_curves: pd.DataFrame | None = None) -> pd.DataFrame:
    snapshot = build_cartera_snapshot(fact_cartera, origin_date)
    if snapshot.empty:
        snapshot = pd.DataFrame(columns=["fecha_objetivo", "tipo_servicio", "cartera_signal"])
    else:
        snapshot = snapshot.merge(scalers, on="tipo_servicio", how="left")
        snapshot["horizonte_dias"] = (pd.to_datetime(snapshot["fecha_objetivo"]) - pd.Timestamp(origin_date).normalize()).dt.days.clip(lower=0)
        if maturity_curves is not None and not maturity_curves.empty:
            snapshot = snapshot.merge(maturity_curves, on=["tipo_servicio", "horizonte_dias"], how="left")
        else:
            snapshot["visible_ratio_pedidos"] = 1.0
        snapshot["visible_ratio_pedidos"] = snapshot["visible_ratio_pedidos"].fillna(1.0).clip(lower=0.05, upper=1.0)
        snapshot["cartera_signal"] = (snapshot["pedidos_abiertos"] / snapshot["visible_ratio_pedidos"]) * snapshot["scaler"].fillna(1.0)
    adjusted = base_forecast.merge(snapshot[["fecha_objetivo", "tipo_servicio", "cartera_signal"]], left_on=["fecha", "tipo_servicio"], right_on=["fecha_objetivo", "tipo_servicio"], how="left")
    adjusted["cartera_signal"] = adjusted["cartera_signal"].fillna(adjusted["forecast"])
    horizon = (pd.to_datetime(adjusted["fecha"]) - pd.Timestamp(origin_date).normalize()).dt.days
    adjusted["horizon_days"] = horizon.astype(int)
    weight_info = adjusted.apply(
        lambda row: pd.Series(
            resolve_nowcasting_weights(
                settings,
                row.get("tipo_servicio"),
                int(row["horizon_days"]),
            )
        ),
        axis=1,
    )
    adjusted["statistical_weight"] = weight_info["statistical_weight"]
    adjusted["cartera_weight"] = weight_info["cartera_weight"]
    adjusted["horizon_bucket"] = weight_info["horizon_bucket"]
    adjusted["forecast"] = adjusted["forecast"] * adjusted["statistical_weight"] + adjusted["cartera_signal"] * adjusted["cartera_weight"]
    return adjusted.drop(columns=["fecha_objetivo"], errors="ignore")
