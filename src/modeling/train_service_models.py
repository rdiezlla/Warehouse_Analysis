from __future__ import annotations

from pathlib import Path

from src.modeling.forecasters import ForecasterConfig, RecursiveForecaster
from src.modeling.model_registry import save_model


def train_service_models(datasets: dict, settings: dict, output_dir: Path) -> dict:
    registry = {}
    random_state = settings["project"]["random_state"]
    for name, df in datasets.items():
        if not (name.startswith("ds_entregas") or name.startswith("ds_recogidas")):
            continue
        frequency = "daily" if name.endswith("_dia") else "weekly"
        lags = settings["features"]["daily_lags"] if frequency == "daily" else settings["features"]["weekly_lags"]
        rolling = settings["features"]["daily_rolling_windows"] if frequency == "daily" else settings["features"]["weekly_rolling_windows"]
        train_df = df[df["flag_periodo"] != "apagado_2025"].copy()
        for family in ["linear", "boosting"]:
            forecaster = RecursiveForecaster(ForecasterConfig(family=family, frequency=frequency, lags=lags, rolling_windows=rolling, random_state=random_state))
            forecaster.fit(train_df)
            artifact = output_dir / f"{name}_{family}.joblib"
            registry[f"{name}::{family}"] = save_model(forecaster, artifact, {"dataset": name, "family": family, "frequency": frequency})
    return registry
