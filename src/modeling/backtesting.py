from __future__ import annotations

import pandas as pd

from src.modeling.baselines import median_by_day_of_week, moving_average, seasonal_naive
from src.modeling.evaluation import build_oof_frame, compute_metrics_by_fold
from src.modeling.forecasters import ForecasterConfig, RecursiveForecaster
from src.modeling.nowcasting import apply_nowcasting, build_cartera_maturity_curves


def _generate_fold_boundaries(df: pd.DataFrame, initial_end: str, horizon: int, step: int, frequency: str) -> list[tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    df = df[df.get("is_actual", 1) == 1].sort_values("fecha")
    initial_end = pd.Timestamp(initial_end)
    max_date = pd.to_datetime(df["fecha"]).max()
    delta = pd.Timedelta(weeks=step) if frequency == "weekly" else pd.Timedelta(days=step)
    horizon_delta = pd.Timedelta(weeks=horizon) if frequency == "weekly" else pd.Timedelta(days=horizon)
    boundaries = []
    origin = initial_end
    while origin + horizon_delta <= max_date:
        test_start = origin + (pd.Timedelta(weeks=1) if frequency == "weekly" else pd.Timedelta(days=1))
        test_end = origin + horizon_delta
        boundaries.append((origin, test_start, test_end))
        origin += delta
    return boundaries


def backtest_dataset(df: pd.DataFrame, settings: dict, frequency: str, include_feature_ablations: bool = False, fact_cartera: pd.DataFrame | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    lags = settings["features"]["daily_lags"] if frequency == "daily" else settings["features"]["weekly_lags"]
    rolling = settings["features"]["daily_rolling_windows"] if frequency == "daily" else settings["features"]["weekly_rolling_windows"]
    initial_end = settings["training"]["initial_train_end_daily"] if frequency == "daily" else settings["training"]["initial_train_end_weekly"]
    horizon = settings["training"]["daily_horizon"] if frequency == "daily" else settings["training"]["weekly_horizon"]
    step = settings["training"]["daily_step_days"] if frequency == "daily" else settings["training"]["weekly_step_weeks"]
    seasonal_lag = 7 if frequency == "daily" else 52
    boundaries = _generate_fold_boundaries(df, initial_end, horizon, step, frequency)
    predictions = []
    model_specs = [("linear_full", "linear", ()), ("boosting_full", "boosting", ())]
    ablation_boundaries = set(boundaries[-12:]) if include_feature_ablations and len(boundaries) > 12 else set(boundaries)
    if include_feature_ablations:
        model_specs.extend([
            ("boosting_no_week_of_month", "boosting", ("week_of_month",)),
            ("boosting_no_post_easter", "boosting", ("is_post_easter_window_1_6",)),
            ("boosting_no_ramp_up", "boosting", ("ramp_up_2026",)),
        ])

    def _fit_fold_cartera_scaler(train_frame: pd.DataFrame, origin_date: pd.Timestamp, service_type: str) -> float:
        if fact_cartera is None:
            return 1.0
        scaler_train = train_frame.copy()
        if "is_final_service_truth" in scaler_train.columns:
            scaler_train = scaler_train[scaler_train["is_final_service_truth"] == 1].copy()
        if scaler_train.empty:
            scaler_train = train_frame.copy()
        cartera_hist = fact_cartera[
            (fact_cartera["tipo_servicio"] == service_type)
            & (fact_cartera["fecha_creacion"] <= origin_date)
            & (fact_cartera["fecha_inicio_evento"] <= origin_date)
        ].copy()
        if cartera_hist.empty:
            return 1.0
        cartera_counts = cartera_hist.groupby("fecha_inicio_evento")["codigo_generico"].nunique().reset_index(name="pedidos_abiertos")
        actual = scaler_train[["fecha", "target"]].copy()
        actual["fecha"] = pd.to_datetime(actual["fecha"])
        merged = actual.merge(cartera_counts, left_on="fecha", right_on="fecha_inicio_evento", how="left")
        merged["pedidos_abiertos"] = merged["pedidos_abiertos"].replace(0, pd.NA)
        merged["ratio"] = merged["target"] / merged["pedidos_abiertos"]
        scaler = merged["ratio"].replace([pd.NA, pd.NaT], pd.NA).dropna()
        return float(scaler.median()) if not scaler.empty else 1.0

    def _fit_fold_maturity_curves(origin_date: pd.Timestamp) -> pd.DataFrame:
        if fact_cartera is None:
            return pd.DataFrame(columns=["tipo_servicio", "horizonte_dias", "visible_ratio_pedidos"])
        historical = fact_cartera[fact_cartera["fecha_inicio_evento"] <= origin_date].copy()
        return build_cartera_maturity_curves(historical, max_horizon=settings["nowcasting"]["medium_horizon_max"])

    for fold_id, (origin, test_start, test_end) in enumerate(boundaries, start=1):
        train = df[(pd.to_datetime(df["fecha"]) <= origin) & (df["flag_periodo"] != "apagado_2025") & (df.get("is_actual", 1) == 1)].copy()
        test = df[(pd.to_datetime(df["fecha"]) >= test_start) & (pd.to_datetime(df["fecha"]) <= test_end) & (df.get("is_actual", 1) == 1)].copy()
        if train.empty or test.empty:
            continue
        history = train.set_index(pd.to_datetime(train["fecha"]))["target"].astype(float)
        future_dates = pd.DatetimeIndex(pd.to_datetime(test["fecha"]))

        baseline_predictions = {
            "seasonal_naive": seasonal_naive(history, future_dates, seasonal_lag),
            "moving_average": moving_average(history, future_dates, 7 if frequency == "daily" else 4),
            "median_dow": median_by_day_of_week(history, future_dates) if frequency == "daily" else moving_average(history, future_dates, 4),
        }
        for model_name, pred_series in baseline_predictions.items():
            predictions.append(build_oof_frame(df["dataset_name"].iloc[0], fold_id, model_name, future_dates, test["target"].values, pred_series.values))

        for model_name, family, drop_features in model_specs:
            if drop_features and (origin, test_start, test_end) not in ablation_boundaries:
                continue
            forecaster = RecursiveForecaster(ForecasterConfig(family=family, frequency=frequency, lags=lags, rolling_windows=rolling, random_state=settings["project"]["random_state"], drop_features=drop_features))
            forecaster.fit(train)
            fold_pred = forecaster.predict(history, test)
            predictions.append(build_oof_frame(df["dataset_name"].iloc[0], fold_id, model_name, fold_pred["fecha"], test["target"].values, fold_pred["prediction"].values))

            if fact_cartera is not None and frequency == "daily" and model_name == "boosting_full" and df["tipo_servicio"].iloc[0] in ["SGE", "SGP", "EGE"]:
                nowcast_input = fold_pred.rename(columns={"prediction": "forecast"}).copy()
                nowcast_input["tipo_servicio"] = df["tipo_servicio"].iloc[0]
                fold_scaler = _fit_fold_cartera_scaler(train, origin, df["tipo_servicio"].iloc[0])
                scalers = pd.DataFrame({"tipo_servicio": [df["tipo_servicio"].iloc[0]], "scaler": [fold_scaler]})
                maturity_curves = _fit_fold_maturity_curves(origin)
                adjusted = apply_nowcasting(nowcast_input, fact_cartera, scalers, origin, settings, maturity_curves=maturity_curves)
                predictions.append(build_oof_frame(df["dataset_name"].iloc[0], fold_id, f"{model_name}__nowcast", adjusted["fecha"], test["target"].values, adjusted["forecast"].values))

    if predictions:
        oof_df = pd.concat(predictions, ignore_index=True)
        return oof_df, compute_metrics_by_fold(oof_df)
    empty_oof = pd.DataFrame(columns=["dataset_name", "fold_id", "model_name", "fecha", "y_true", "y_pred", "abs_error", "is_peak"])
    return empty_oof, compute_metrics_by_fold(empty_oof)
