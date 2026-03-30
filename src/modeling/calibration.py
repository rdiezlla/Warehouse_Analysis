from __future__ import annotations

import copy

import numpy as np
import pandas as pd


def daily_horizon_bucket(horizon_days: int) -> str | None:
    if horizon_days == 1:
        return "D+1"
    if 2 <= horizon_days <= 3:
        return "D+2_a_D+3"
    if 4 <= horizon_days <= 7:
        return "D+4_a_D+7"
    if 8 <= horizon_days <= 14:
        return "D+8_a_D+14"
    if 15 <= horizon_days <= 28:
        return "D+15_a_D+28"
    return None


def weekly_horizon_bucket(horizon_weeks: int) -> str | None:
    if horizon_weeks == 1:
        return "W+1"
    if horizon_weeks == 2:
        return "W+2"
    if 3 <= horizon_weeks <= 4:
        return "W+3_a_W+4"
    if horizon_weeks >= 5:
        return "W+5+"
    return None


def _normalize_weights(statistical_weight: float, cartera_weight: float) -> tuple[float, float]:
    total = float(statistical_weight) + float(cartera_weight)
    if total <= 0:
        return 1.0, 0.0
    return float(statistical_weight) / total, float(cartera_weight) / total


def resolve_nowcasting_weights(settings: dict, tipo_servicio: str | None, horizon_days: int) -> dict[str, float | str | None]:
    nowcasting = settings.get("nowcasting", {})
    short_horizon_max = int(nowcasting.get("short_horizon_max", 7))
    medium_horizon_max = int(nowcasting.get("medium_horizon_max", 28))
    horizon_bucket = daily_horizon_bucket(int(horizon_days))

    if 0 <= int(horizon_days) <= short_horizon_max:
        service_weights = nowcasting.get("kpi_weights", {}).get(str(tipo_servicio), {})
        bucket_weights = service_weights.get(horizon_bucket, {}) if horizon_bucket else {}
        statistical_weight = bucket_weights.get("statistical_weight_short", nowcasting.get("statistical_weight_short", 1.0))
        cartera_weight = bucket_weights.get("cartera_weight_short", nowcasting.get("cartera_weight_short", 0.0))
    elif short_horizon_max < int(horizon_days) <= medium_horizon_max:
        statistical_weight = nowcasting.get("statistical_weight_medium", 1.0)
        cartera_weight = nowcasting.get("cartera_weight_medium", 0.0)
    else:
        statistical_weight = 1.0
        cartera_weight = 0.0

    statistical_weight, cartera_weight = _normalize_weights(statistical_weight, cartera_weight)
    return {
        "statistical_weight": statistical_weight,
        "cartera_weight": cartera_weight,
        "horizon_bucket": horizon_bucket,
    }


def apply_daily_postprocess(forecast_df: pd.DataFrame, settings: dict) -> pd.DataFrame:
    if forecast_df.empty:
        return forecast_df.copy()

    output = forecast_df.copy()
    output["forecast"] = pd.to_numeric(output["forecast"], errors="coerce").fillna(0.0)
    policies = settings.get("calibration", {}).get("daily_postprocess", {})

    for kpi, policy in policies.items():
        mask = output["kpi"] == kpi
        if not mask.any():
            continue
        add_bias = float(policy.get("add_bias", 0.0) or 0.0)
        multiplier = float(policy.get("multiplier", 1.0) or 1.0)
        clip_min = float(policy.get("clip_min", 0.0) or 0.0)
        clip_max = policy.get("clip_max")
        output.loc[mask, "forecast"] = output.loc[mask, "forecast"] * multiplier + add_bias
        output.loc[mask, "forecast"] = output.loc[mask, "forecast"].clip(lower=clip_min)
        if clip_max is not None:
            output.loc[mask, "forecast"] = output.loc[mask, "forecast"].clip(upper=float(clip_max))
        if bool(policy.get("round", False)):
            output.loc[mask, "forecast"] = np.rint(output.loc[mask, "forecast"])

    output["forecast"] = output["forecast"].clip(lower=0.0)
    return output


def build_baseline_calibration_settings(settings: dict) -> dict:
    baseline_settings = copy.deepcopy(settings)
    baseline_reference = baseline_settings.get("calibration", {}).get("baseline_reference", {})
    baseline_nowcasting = baseline_reference.get("nowcasting", {})
    baseline_settings.setdefault("nowcasting", {})
    baseline_settings["nowcasting"].update(baseline_nowcasting)
    baseline_settings["nowcasting"].pop("kpi_weights", None)
    baseline_settings.setdefault("calibration", {})
    baseline_settings["calibration"]["daily_postprocess"] = baseline_reference.get("daily_postprocess", {})
    return baseline_settings
