from __future__ import annotations

import pandas as pd

from src.modeling.backtesting import _generate_fold_boundaries
from src.modeling.baselines import median_by_day_of_week, moving_average, seasonal_naive
from src.modeling.evaluation import compute_metrics
from src.modeling.forecasters import ForecasterConfig, RecursiveForecaster


def fit_transformer(movimientos_albaranes: pd.DataFrame, albaranes: pd.DataFrame) -> dict[str, pd.DataFrame]:
    matched = movimientos_albaranes[(movimientos_albaranes["tipo_movimiento"] == "PI") & (movimientos_albaranes["match_albaranes"])].copy()
    matched = matched.dropna(subset=["fecha_operativa_mov", "fecha_servicio"])
    matched["fecha_servicio"] = matched["fecha_servicio"].dt.normalize()
    matched["fecha_operativa_mov"] = matched["fecha_operativa_mov"].dt.normalize()
    matched["offset_dias"] = (matched["fecha_servicio"] - matched["fecha_operativa_mov"]).dt.days
    matched = matched[matched["offset_dias"].between(0, 3)]

    service_level = (
        matched.groupby(["pedido_externo", "tipo_servicio", "urgencia_norm", "fecha_servicio"]).agg(
            pi_lines=("tipo_movimiento", "size"),
            pi_units=("cantidad", "sum"),
        )
        .reset_index()
    )
    intensity = (
        service_level.groupby(["tipo_servicio", "urgencia_norm"]).agg(
            intensity_lines=("pi_lines", "mean"),
            intensity_units=("pi_units", "mean"),
            matched_services=("pedido_externo", "nunique"),
        )
        .reset_index()
    )
    offset_probs = matched.groupby(["tipo_servicio", "urgencia_norm", "offset_dias"]).size().reset_index(name="n")
    offset_probs["prob_offset"] = offset_probs.groupby(["tipo_servicio", "urgencia_norm"])["n"].transform(lambda x: x / x.sum())
    urgency_mix = albaranes.groupby(["tipo_servicio", "urgencia_norm"]).size().reset_index(name="n")
    urgency_mix["urgency_share"] = urgency_mix.groupby("tipo_servicio")["n"].transform(lambda x: x / x.sum())
    global_curve = matched.groupby("offset_dias").size().reset_index(name="n")
    global_curve["prob_offset"] = global_curve["n"] / global_curve["n"].sum()
    return {"intensity": intensity, "offset_probs": offset_probs, "urgency_mix": urgency_mix, "global_curve": global_curve}


def apply_transformer(service_forecasts: pd.DataFrame, transformer: dict[str, pd.DataFrame], metric: str = "lines") -> pd.DataFrame:
    intensity_col = "intensity_lines" if metric == "lines" else "intensity_units"
    records = []
    for _, row in service_forecasts.iterrows():
        service_type = row["tipo_servicio"]
        date = pd.Timestamp(row["fecha"])
        urg_mix = transformer["urgency_mix"].query("tipo_servicio == @service_type")
        if urg_mix.empty:
            urg_mix = pd.DataFrame({"urgencia_norm": ["UNKNOWN"], "urgency_share": [1.0]})
        for _, urg in urg_mix.iterrows():
            intensity = transformer["intensity"].query("tipo_servicio == @service_type and urgencia_norm == @urg.urgencia_norm")
            if intensity.empty:
                continue
            intensity_value = float(intensity[intensity_col].iloc[0])
            offsets = transformer["offset_probs"].query("tipo_servicio == @service_type and urgencia_norm == @urg.urgencia_norm")
            if offsets.empty:
                offsets = transformer["global_curve"]
            for _, offset_row in offsets.iterrows():
                records.append(
                    {
                        "fecha": date - pd.Timedelta(days=int(offset_row["offset_dias"])),
                        "tipo_servicio": service_type,
                        "forecast": float(row["forecast"]) * float(urg["urgency_share"]) * intensity_value * float(offset_row["prob_offset"]),
                    }
                )
    if not records:
        return pd.DataFrame(columns=["fecha", "tipo_servicio", "forecast"])
    return pd.DataFrame(records).groupby(["fecha", "tipo_servicio"], as_index=False)["forecast"].sum()


def _predict_single_model(dataset: pd.DataFrame, settings: dict, model_name: str, frequency: str, origin: pd.Timestamp, test: pd.DataFrame) -> pd.DataFrame:
    history = dataset[(pd.to_datetime(dataset["fecha"]) <= origin) & (dataset["flag_periodo"] != "apagado_2025") & (dataset["is_actual"] == 1)].copy()
    history_series = history.set_index(pd.to_datetime(history["fecha"]))["target"].astype(float)
    future_dates = pd.DatetimeIndex(pd.to_datetime(test["fecha"]))
    if model_name == "seasonal_naive":
        pred = seasonal_naive(history_series, future_dates, 7 if frequency == "daily" else 52)
        return pd.DataFrame({"fecha": future_dates, "forecast": pred.values})
    if model_name == "moving_average":
        pred = moving_average(history_series, future_dates, 7 if frequency == "daily" else 4)
        return pd.DataFrame({"fecha": future_dates, "forecast": pred.values})
    if model_name == "median_dow":
        pred = median_by_day_of_week(history_series, future_dates) if frequency == "daily" else moving_average(history_series, future_dates, 4)
        return pd.DataFrame({"fecha": future_dates, "forecast": pred.values})

    drop_features = ()
    family = "boosting" if "boosting" in model_name else "linear"
    if "no_week_of_month" in model_name:
        drop_features = ("week_of_month",)
    elif "no_post_easter" in model_name:
        drop_features = ("is_post_easter_window_1_6",)
    elif "no_ramp_up" in model_name:
        drop_features = ("ramp_up_2026",)
    lags = settings["features"]["daily_lags"] if frequency == "daily" else settings["features"]["weekly_lags"]
    rolling = settings["features"]["daily_rolling_windows"] if frequency == "daily" else settings["features"]["weekly_rolling_windows"]
    forecaster = RecursiveForecaster(ForecasterConfig(family=family, frequency=frequency, lags=lags, rolling_windows=rolling, random_state=settings["project"]["random_state"], drop_features=drop_features))
    forecaster.fit(history)
    return forecaster.predict(history_series, test).rename(columns={"prediction": "forecast"})


def compare_direct_vs_transformer_last_fold(
    service_datasets: dict[str, pd.DataFrame],
    picking_datasets: dict[str, pd.DataFrame],
    metrics: pd.DataFrame,
    predictions: pd.DataFrame,
    movimientos_albaranes: pd.DataFrame,
    albaranes: pd.DataFrame,
    settings: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    picking_lines = picking_datasets["ds_picking_lines_pi_dia"]
    boundaries = _generate_fold_boundaries(
        picking_lines,
        settings["training"]["initial_train_end_daily"],
        settings["training"]["daily_horizon"],
        settings["training"]["daily_step_days"],
        "daily",
    )
    if not boundaries:
        return pd.DataFrame(), pd.DataFrame()
    origin, test_start, test_end = boundaries[-1]

    albaranes_train = albaranes[(albaranes["fecha_servicio"] <= origin) & (albaranes["fecha_servicio"].dt.year != 2025)].copy()
    mov_train = movimientos_albaranes[
        (movimientos_albaranes["fecha_operativa_mov"] <= origin)
        & (movimientos_albaranes["fecha_operativa_mov"].dt.year != 2025)
    ].copy()
    transformer = fit_transformer(mov_train, albaranes_train)

    service_forecasts = []
    for dataset_name, tipo_servicio in [
        ("ds_entregas_sge_dia", "SGE"),
        ("ds_entregas_sgp_dia", "SGP"),
        ("ds_recogidas_ege_dia", "EGE"),
    ]:
        dataset = service_datasets[dataset_name]
        test = dataset[(pd.to_datetime(dataset["fecha"]) >= test_start) & (pd.to_datetime(dataset["fecha"]) <= test_end) & (dataset["is_actual"] == 1)].copy()
        best_model = (
            metrics[(metrics["dataset_name"] == dataset_name) & (~metrics["model_name"].str.contains("nowcast", na=False))]
            .groupby("model_name")["wape"]
            .mean()
            .sort_values()
            .index[0]
        )
        pred = _predict_single_model(dataset, settings, best_model, "daily", origin, test)
        pred["tipo_servicio"] = tipo_servicio
        service_forecasts.append(pred)
    service_forecasts = pd.concat(service_forecasts, ignore_index=True)

    transformed_lines = apply_transformer(service_forecasts, transformer, metric="lines")
    transformed_units = apply_transformer(service_forecasts, transformer, metric="units")

    comparisons = []
    metrics_rows = []
    for dataset_name, transformed_df, metric_name in [
        ("ds_picking_lines_pi_dia", transformed_lines, "transformer_lines"),
        ("ds_picking_units_pi_dia", transformed_units, "transformer_units"),
    ]:
        dataset = picking_datasets[dataset_name]
        test = dataset[(pd.to_datetime(dataset["fecha"]) >= test_start) & (pd.to_datetime(dataset["fecha"]) <= test_end) & (dataset["is_actual"] == 1)].copy()
        best_direct_model = (
            metrics[metrics["dataset_name"] == dataset_name]
            .groupby("model_name")["wape"]
            .mean()
            .sort_values()
            .index[0]
        )
        direct_pred = predictions[
            (predictions["dataset_name"] == dataset_name)
            & (predictions["fold_id"] == predictions.loc[predictions["dataset_name"] == dataset_name, "fold_id"].max())
            & (predictions["model_name"] == best_direct_model)
        ][["fecha", "prediction"]].rename(columns={"prediction": "forecast"})
        direct_pred["fecha"] = pd.to_datetime(direct_pred["fecha"])
        test["fecha"] = pd.to_datetime(test["fecha"])
        transformed_df["fecha"] = pd.to_datetime(transformed_df["fecha"])

        transformed_eval = test[["fecha", "target"]].merge(transformed_df[["fecha", "forecast"]], on="fecha", how="left").fillna({"forecast": 0.0})
        direct_eval = test[["fecha", "target"]].merge(direct_pred, on="fecha", how="left").fillna({"forecast": 0.0})

        comparisons.extend([
            transformed_eval.assign(metric=dataset_name, source="transformer"),
            direct_eval.assign(metric=dataset_name, source="direct"),
        ])
        metrics_rows.extend([
            {"metric": dataset_name, "source": "transformer", **compute_metrics(transformed_eval["target"], transformed_eval["forecast"])},
            {"metric": dataset_name, "source": "direct", **compute_metrics(direct_eval["target"], direct_eval["forecast"])},
        ])

    return pd.concat(comparisons, ignore_index=True), pd.DataFrame(metrics_rows)
