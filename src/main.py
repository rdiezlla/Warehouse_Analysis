from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from src.data.calendar_builder import build_dim_date
from src.data.clean_albaranes import clean_albaranes
from src.data.clean_maestro import clean_maestro
from src.data.clean_movimientos import clean_movimientos
from src.data.clean_solicitudes import clean_solicitudes
from src.data.fact_builder import build_fact_cartera, build_fact_picking_dia, build_fact_servicio_dia, build_weekly_fact
from src.data.ingest import ingest_sources
from src.data.joiner import build_join_outputs
from src.features.transformer_features import build_transformer_training_table
from src.logging_config import setup_logging
from src.modeling.backtesting import backtest_dataset
from src.modeling.datasets import build_and_save_datasets
from src.modeling.evaluation import rank_models, summarize_oof_predictions
from src.modeling.feature_policy import load_feature_policy
from src.modeling.forecasters import ForecasterConfig, RecursiveForecaster
from src.modeling.hierarchical_reconciliation import reconcile_weekly_from_daily
from src.modeling.model_registry import save_registry
from src.modeling.nowcasting import apply_nowcasting, build_cartera_maturity_curves, fit_cartera_scalers
from src.modeling.train_picking_models import train_picking_models
from src.modeling.train_service_models import train_service_models
from src.modeling.transformer_service_to_picking import apply_transformer, compare_direct_vs_transformer_last_fold, fit_transformer
from src.paths import BACKTESTS_DIR, CONFIG_DIR, FEATURES_DIR, FORECASTS_DIR, INTERIM_DIR, MODELS_DIR, PLOTS_DIR, PROCESSED_DIR, QA_DIR, REPORTS_DIR
from src.reporting.explainability import export_model_explainability
from src.reporting.plot_backtests import plot_backtest_errors
from src.reporting.plot_forecasts import plot_history_and_forecast, plot_transformer_curve
from src.reporting.qa_reports import save_join_reports, write_qa_report
from src.utils.io_utils import ensure_dirs, load_yaml, save_dataframe, save_parquet_safe

LOGGER = logging.getLogger(__name__)

BASELINE_MODELS = {"seasonal_naive", "moving_average", "median_dow"}


def load_config() -> tuple[dict, dict, dict]:
    settings = load_yaml(CONFIG_DIR / "settings.yaml")
    aliases = load_yaml(CONFIG_DIR / "column_aliases.yaml")
    regex_rules = load_yaml(CONFIG_DIR / "regex_rules.yaml")
    return settings, aliases, regex_rules


def _save_interim_tables(albaranes, movimientos, solicitudes, maestro_raw, maestro_dedup) -> None:
    save_dataframe(albaranes, INTERIM_DIR / "albaranes_clean", index=False)
    save_dataframe(movimientos, INTERIM_DIR / "movimientos_clean", index=False)
    save_dataframe(solicitudes, INTERIM_DIR / "solicitudes_clean", index=False)
    save_dataframe(maestro_raw, INTERIM_DIR / "maestro_clean", index=False)
    save_dataframe(maestro_dedup, INTERIM_DIR / "maestro_dedup", index=False)


def run_qa(settings: dict, aliases: dict, regex_rules: dict) -> dict:
    ensure_dirs(INTERIM_DIR, PROCESSED_DIR, QA_DIR, REPORTS_DIR)
    raw = ingest_sources(settings)
    albaranes, qa_alb = clean_albaranes(raw["albaranes"], aliases, regex_rules)
    movimientos, qa_mov = clean_movimientos(raw["movimientos"], aliases)
    solicitudes, qa_sol = clean_solicitudes(raw["solicitudes"], aliases, regex_rules)
    maestro_raw, maestro_dedup, qa_mae = clean_maestro(raw["maestro"], aliases)
    _save_interim_tables(albaranes, movimientos, solicitudes, maestro_raw, maestro_dedup)

    dim_date = build_dim_date(settings)
    save_dataframe(dim_date, PROCESSED_DIR / "dim_date", index=False)

    joins = build_join_outputs(albaranes, movimientos, solicitudes, maestro_dedup)
    save_join_reports(joins, QA_DIR)
    save_parquet_safe(joins["movimientos_albaranes"], PROCESSED_DIR / "movimientos_albaranes.parquet", index=False)
    save_parquet_safe(joins["solicitudes_maestro"], PROCESSED_DIR / "solicitudes_maestro.parquet", index=False)

    fact_servicio_dia = build_fact_servicio_dia(albaranes, dim_date)
    fact_picking_dia, fact_other_movs = build_fact_picking_dia(joins["movimientos_maestro"], settings["thresholds"]["heavy_item_kg"], settings["thresholds"]["bulky_item_m3"])
    fact_cartera, cartera_fecha_objetivo, cartera_horizonte = build_fact_cartera(joins["solicitudes_maestro"])
    cartera_maturity_curves = build_cartera_maturity_curves(fact_cartera, max_horizon=settings["nowcasting"]["medium_horizon_max"])

    save_dataframe(fact_servicio_dia, PROCESSED_DIR / "fact_servicio_dia", index=False)
    save_dataframe(fact_picking_dia, PROCESSED_DIR / "fact_picking_dia", index=False)
    save_dataframe(fact_other_movs, PROCESSED_DIR / "fact_movimientos_aux_dia", index=False)
    save_dataframe(fact_cartera, PROCESSED_DIR / "fact_cartera", index=False)
    save_dataframe(cartera_fecha_objetivo, PROCESSED_DIR / "cartera_por_fecha_objetivo", index=False)
    save_dataframe(cartera_horizonte, PROCESSED_DIR / "cartera_por_horizonte", index=False)
    save_dataframe(cartera_maturity_curves, PROCESSED_DIR / "cartera_maturity_curves", index=False)
    save_dataframe(build_weekly_fact(fact_servicio_dia), PROCESSED_DIR / "fact_servicio_semana", index=False)
    save_dataframe(build_weekly_fact(fact_picking_dia), PROCESSED_DIR / "fact_picking_semana", index=False)

    transformer_training = build_transformer_training_table(joins["movimientos_albaranes"])
    save_dataframe(transformer_training, PROCESSED_DIR / "transformer_training_table", index=False)

    classification_qa = pd.DataFrame([
        {"metric": "pct_unk", "value": qa_alb["service_classification"]["pct_unk"]},
        *[{"metric": f"unknown::{key}", "value": value} for key, value in qa_alb["service_classification"]["top_unknown_patterns"].items()],
    ])
    classification_qa.to_csv(QA_DIR / "service_classification_qa.csv", index=False)

    qa_payload = {
        "albaranes": qa_alb,
        "movimientos": qa_mov,
        "solicitudes": qa_sol,
        "maestro": qa_mae,
        "joins": joins["coverage_global"].to_dict(orient="records"),
    }
    write_qa_report(qa_payload, REPORTS_DIR / "qa_summary.md")
    return {
        "albaranes": albaranes,
        "movimientos": movimientos,
        "solicitudes": solicitudes,
        "maestro_dedup": maestro_dedup,
        "dim_date": dim_date,
        "fact_servicio_dia": fact_servicio_dia,
        "fact_picking_dia": fact_picking_dia,
        "fact_cartera": fact_cartera,
        "cartera_maturity_curves": cartera_maturity_curves,
        "joins": joins,
    }


def _read_processed_table(name: str) -> pd.DataFrame:
    parquet_path = PROCESSED_DIR / f"{name}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)
    return pd.read_parquet(parquet_path)


def ensure_processed_context(settings: dict, aliases: dict, regex_rules: dict) -> dict:
    required = [PROCESSED_DIR / "fact_servicio_dia.parquet", PROCESSED_DIR / "fact_picking_dia.parquet", PROCESSED_DIR / "fact_cartera.parquet", PROCESSED_DIR / "dim_date.parquet"]
    if not all(path.exists() for path in required):
        return run_qa(settings, aliases, regex_rules)
    return {
        "fact_servicio_dia": _read_processed_table("fact_servicio_dia"),
        "fact_picking_dia": _read_processed_table("fact_picking_dia"),
        "fact_cartera": _read_processed_table("fact_cartera"),
        "cartera_maturity_curves": _read_processed_table("cartera_maturity_curves"),
        "dim_date": _read_processed_table("dim_date"),
        "albaranes": pd.read_parquet(INTERIM_DIR / "albaranes_clean.parquet"),
        "joins": {"movimientos_albaranes": pd.read_parquet(PROCESSED_DIR / "movimientos_albaranes.parquet")},
    }


def run_features(settings: dict, aliases: dict, regex_rules: dict) -> dict:
    context = ensure_processed_context(settings, aliases, regex_rules)
    datasets = build_and_save_datasets(context["fact_servicio_dia"], context["fact_picking_dia"], context["dim_date"], settings, FEATURES_DIR)
    dataset_service_type_qa = pd.DataFrame(
        [
            {"dataset_name": name, "tipo_servicio": df["tipo_servicio"].iloc[0]}
            for name, df in datasets.items()
            if "tipo_servicio" in df.columns
        ]
    )
    dataset_service_type_qa.to_csv(QA_DIR / "dataset_tipo_servicio_qa.csv", index=False)
    return datasets


def load_datasets(settings: dict, aliases: dict, regex_rules: dict) -> dict:
    if not FEATURES_DIR.exists() or not any(FEATURES_DIR.glob("*.parquet")):
        return run_features(settings, aliases, regex_rules)
    datasets = {}
    for path in FEATURES_DIR.glob("*.parquet"):
        datasets[path.stem] = pd.read_parquet(path)
    return datasets


def run_train(settings: dict, aliases: dict, regex_rules: dict) -> dict:
    ensure_dirs(MODELS_DIR)
    datasets = load_datasets(settings, aliases, regex_rules)
    registry = {}
    registry.update(train_service_models(datasets, settings, MODELS_DIR))
    registry.update(train_picking_models(datasets, settings, MODELS_DIR))
    save_registry(registry, MODELS_DIR / "model_registry.json")
    export_model_explainability(registry, REPORTS_DIR / "model_registry_summary.csv")
    return registry


def run_backtest(settings: dict, aliases: dict, regex_rules: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    ensure_dirs(BACKTESTS_DIR)
    datasets = load_datasets(settings, aliases, regex_rules)
    context = ensure_processed_context(settings, aliases, regex_rules)
    all_oof = []
    all_metrics = []
    for name, df in datasets.items():
        frequency = "daily" if name.endswith("_dia") else "weekly"
        include_ablations = frequency == "daily"
        fact_cartera = context["fact_cartera"] if (frequency == "daily" and (name.startswith("ds_entregas") or name.startswith("ds_recogidas"))) else None
        oof_predictions, metrics = backtest_dataset(df, settings, frequency, include_feature_ablations=include_ablations, fact_cartera=fact_cartera)
        all_oof.append(oof_predictions)
        all_metrics.append(metrics)
        oof_predictions.to_csv(BACKTESTS_DIR / f"backtest_predictions_{name}.csv", index=False)
        metrics.to_csv(BACKTESTS_DIR / f"backtest_metrics_{name}.csv", index=False)
        if not oof_predictions.empty:
            plot_backtest_errors(oof_predictions, f"Backtest errors {name}", PLOTS_DIR / f"backtest_errors_{name}.png")
    combined_predictions = pd.concat(all_oof, ignore_index=True) if all_oof else pd.DataFrame(columns=["dataset_name", "fold_id", "model_name", "fecha", "y_true", "y_pred", "abs_error", "is_peak"])
    combined_metrics = pd.concat(all_metrics, ignore_index=True) if all_metrics else pd.DataFrame()
    combined_predictions.to_csv(BACKTESTS_DIR / "backtest_predictions_all.csv", index=False)
    combined_predictions[["dataset_name", "fold_id", "model_name", "fecha", "y_true", "y_pred", "abs_error", "is_peak"]].to_csv(
        BACKTESTS_DIR / "out-of-fold_predictions.csv",
        index=False,
    )
    combined_metrics.to_csv(BACKTESTS_DIR / "backtest_metrics_all.csv", index=False)
    model_summary = summarize_oof_predictions(combined_predictions)
    principal_summary = summarize_oof_predictions(combined_predictions, evaluation_view="ranking_operativo_principal")
    stress_summary = summarize_oof_predictions(combined_predictions, evaluation_view="stress_test_2025_2026")
    model_ranking = rank_models(principal_summary)
    stress_ranking = rank_models(stress_summary)
    model_summary.to_csv(BACKTESTS_DIR / "backtest_model_summary_real_scale.csv", index=False)
    principal_summary.to_csv(BACKTESTS_DIR / "backtest_model_summary_operativo_principal.csv", index=False)
    stress_summary.to_csv(BACKTESTS_DIR / "backtest_model_summary_stress_test.csv", index=False)
    model_ranking.to_csv(BACKTESTS_DIR / "backtest_model_ranking.csv", index=False)
    model_ranking.to_csv(BACKTESTS_DIR / "ranking_operativo_principal.csv", index=False)
    stress_ranking.to_csv(BACKTESTS_DIR / "ranking_stress_test.csv", index=False)
    _write_feature_training_policy()
    _write_backtesting_audit(model_summary, principal_summary, stress_summary)
    _write_model_selection_recommendation(model_ranking, stress_ranking)

    transformer_cmp, transformer_metrics = compare_direct_vs_transformer_last_fold(
        {name: df for name, df in datasets.items() if name.startswith("ds_entregas") or name.startswith("ds_recogidas")},
        {name: df for name, df in datasets.items() if name.startswith("ds_picking")},
        combined_metrics,
        combined_predictions,
        context["joins"]["movimientos_albaranes"],
        context["albaranes"],
        settings,
    )
    if not transformer_cmp.empty:
        transformer_cmp.to_csv(BACKTESTS_DIR / "backtest_transformer_vs_direct_last_fold.csv", index=False)
        transformer_metrics.to_csv(BACKTESTS_DIR / "backtest_transformer_vs_direct_metrics.csv", index=False)
    return combined_predictions, combined_metrics


def _write_feature_training_policy() -> None:
    policy = load_feature_policy()
    lines = [
        "# Feature Training Policy",
        "",
        "## Columnas permitidas",
        *[f"- `{item}`" for item in policy.get("allow_exact", [])],
        "",
        "## Prefijos permitidos",
        *[f"- `{item}*`" for item in policy.get("allow_prefixes", [])],
        "",
        "## Columnas prohibidas",
        *[f"- `{item}`: columna de control, auditoría o target/resultado." for item in policy.get("block_exact", [])],
        "",
        "## Prefijos prohibidos",
        *[f"- `{item}*`: columnas de control, flags o trazabilidad no válidas para X." for item in policy.get("block_prefixes", [])],
        "",
        "## Reglas",
        "- `is_actual` solo se usa como filtro y auditoría, nunca como predictor.",
        "- `tipo_servicio` queda para joins y nowcasting, no entra en X del modelo tabular por KPI.",
        "- Cualquier columna fuera de allowlist dispara error en entrenamiento o predicción.",
    ]
    (REPORTS_DIR / "feature_training_policy.md").write_text("\n".join(lines), encoding="utf-8")


def _write_backtesting_audit(model_summary: pd.DataFrame, principal_summary: pd.DataFrame, stress_summary: pd.DataFrame) -> None:
    lines = [
        "# Backtesting Audit",
        "",
        "## Bugs corregidos",
        "- `is_actual` bloqueado como feature; solo sirve para filtrar historia observada real.",
        "- `tipo_servicio` en datasets de servicios ahora usa mapping determinista SGE/SGP/EGE.",
        "- `fact_servicio_dia` ya no expande calendario completo; la expansión ocurre solo en datasets.",
        "- Métricas, OOF, ranking y plots salen de la misma base `out-of-fold_predictions.csv`.",
        "- WAPE y WAPE peak devuelven `NaN` en ventanas muertas sin señal, y no puntúan como éxito perfecto.",
        "- 2025/2026 salen del ranking operativo principal y pasan a vista separada de stress test.",
        "- Nowcasting en backtest calibra scaler por fold con historia disponible y usa `tipo_servicio` correcto.",
        "",
        "## Política 2025",
        "- Ranking operativo principal: solo OOF con fechas <= 2024-12-31.",
        "- Stress test: OOF con fechas >= 2025-01-01, incluyendo 2025 y 2026.",
        "",
        "## Política WAPE ventanas muertas",
        "- Si `sum(abs(y_true)) == 0`, `WAPE = NaN`.",
        "- Si el subconjunto peak no tiene denominador válido, `wape_peak = NaN`.",
        "- Los rankings usan medias de pandas, por lo que estas ventanas no sesgan el promedio.",
        "",
        "## Columnas prohibidas",
        "- Ver `feature_training_policy.md`.",
        "",
        "## Artefactos",
        f"- OOF: {BACKTESTS_DIR / 'out-of-fold_predictions.csv'}",
        f"- Fold metrics: {BACKTESTS_DIR / 'backtest_metrics_all.csv'}",
        f"- Summary all: {BACKTESTS_DIR / 'backtest_model_summary_real_scale.csv'}",
        f"- Ranking principal: {BACKTESTS_DIR / 'ranking_operativo_principal.csv'}",
        f"- Ranking stress: {BACKTESTS_DIR / 'ranking_stress_test.csv'}",
        "",
        "## Tamaños",
        f"- Model summary all rows: {len(model_summary)}",
        f"- Ranking principal rows: {len(principal_summary)}",
        f"- Stress summary rows: {len(stress_summary)}",
    ]
    (REPORTS_DIR / "backtesting_audit.md").write_text("\n".join(lines), encoding="utf-8")


def _write_model_selection_recommendation(principal_ranking: pd.DataFrame, stress_ranking: pd.DataFrame) -> None:
    lines = ["# Model Selection Recommendation", ""]
    for dataset_name, group in principal_ranking.groupby("dataset_name"):
        ordered = group.sort_values("rank")
        best = ordered.iloc[0]
        second = ordered.iloc[1] if len(ordered) > 1 else ordered.iloc[0]
        stress_best = stress_ranking[stress_ranking["dataset_name"] == dataset_name].sort_values("rank")
        stress_note = ""
        if not stress_best.empty and stress_best.iloc[0]["model_name"] != best["model_name"]:
            stress_note = f" Cambio en stress test: gana `{stress_best.iloc[0]['model_name']}`."
        lines.extend(
            [
                f"## {dataset_name}",
                f"- Modelo recomendado: `{best['model_name']}`",
                f"- Segundo mejor: `{second['model_name']}`",
                "- Métrica principal usada: `mean_wape` sobre ranking operativo principal",
                f"- Trade-off robustez/precisión: `mean_mae={best['mean_mae']:.3f}`, `p90_abs_error={best['p90_abs_error']:.3f}`.{stress_note}",
                f"- Tipo recomendado: {'baseline' if best['model_name'] in BASELINE_MODELS else 'modelo complejo'}",
                f"- Sobreajuste/inestabilidad: {'sí, revisar stress test' if stress_note else 'sin señal fuerte de inestabilidad frente al stress test'}",
                "",
            ]
        )
    (REPORTS_DIR / "model_selection_recommendation.md").write_text("\n".join(lines), encoding="utf-8")


def _best_model_name(metrics: pd.DataFrame, dataset_name: str) -> str:
    ranking_path = BACKTESTS_DIR / "ranking_operativo_principal.csv"
    if ranking_path.exists():
        ranking = pd.read_csv(ranking_path)
        subset = ranking[ranking["dataset_name"] == dataset_name].sort_values("rank")
        if not subset.empty:
            return str(subset.iloc[0]["model_name"])
    if metrics.empty:
        return "boosting_full"
    subset = metrics[(metrics["dataset_name"] == dataset_name) & (~metrics["model_name"].str.contains("nowcast", na=False))].copy()
    if "evaluation_view" in subset.columns:
        subset = subset[subset["evaluation_view"] == "ranking_operativo_principal"].copy()
    if subset.empty:
        return "boosting_full"
    ranking = subset.groupby("model_name", as_index=False)["wape"].mean().sort_values("wape", na_position="last")
    return str(ranking.iloc[0]["model_name"])


def _fit_best_forecaster(df: pd.DataFrame, settings: dict, frequency: str, model_name: str) -> RecursiveForecaster:
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
    forecaster.fit(df[(df["flag_periodo"] != "apagado_2025") & (df.get("is_actual", 1) == 1)].copy())
    return forecaster


def run_forecast(settings: dict, aliases: dict, regex_rules: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    ensure_dirs(FORECASTS_DIR, PLOTS_DIR)
    context = ensure_processed_context(settings, aliases, regex_rules)
    datasets = load_datasets(settings, aliases, regex_rules)
    metrics_path = BACKTESTS_DIR / "backtest_metrics_all.csv"
    metrics = pd.read_csv(metrics_path) if metrics_path.exists() else pd.DataFrame()

    service_daily_forecasts = []
    direct_picking_forecasts = []
    weekly_forecasts = []

    for name, df in datasets.items():
        frequency = "daily" if name.endswith("_dia") else "weekly"
        actual_mask = df.get("is_actual", 1) == 1
        latest_actual = pd.to_datetime(df.loc[actual_mask, "fecha"]).max()
        horizon = settings["training"]["daily_horizon"] if frequency == "daily" else settings["training"]["weekly_horizon"]
        future = df[pd.to_datetime(df["fecha"]) > latest_actual].head(horizon).copy()
        if future.empty:
            continue
        history_df = df.loc[(pd.to_datetime(df["fecha"]) <= latest_actual) & (df.get("is_actual", 1) == 1)].copy()
        history = history_df.set_index(pd.to_datetime(history_df["fecha"]))["target"].astype(float)
        model_name = _best_model_name(metrics, name)
        forecaster = _fit_best_forecaster(df, settings, frequency, model_name)
        forecast = forecaster.predict(history, future)
        forecast["dataset_name"] = name
        forecast["model_name"] = model_name

        if name.startswith("ds_entregas") or name.startswith("ds_recogidas"):
            tipo_servicio = df["tipo_servicio"].iloc[0]
            if frequency == "daily":
                forecast = forecast.rename(columns={"prediction": "forecast"})
                forecast["tipo_servicio"] = tipo_servicio
                scalers = fit_cartera_scalers(context["fact_servicio_dia"], context["fact_cartera"])
                forecast = apply_nowcasting(forecast, context["fact_cartera"], scalers, latest_actual, settings, maturity_curves=context["cartera_maturity_curves"])
                forecast["kpi"] = name.replace("ds_", "").replace("_dia", "")
                service_daily_forecasts.append(forecast[["fecha", "tipo_servicio", "forecast", "kpi", "model_name"]])
                plot_history_and_forecast(history_df[["fecha", "target"]].tail(120), forecast[["fecha", "forecast"]], name, PLOTS_DIR / f"forecast_{name}.png")
            else:
                weekly_forecasts.append(forecast.rename(columns={"prediction": "forecast"}).assign(kpi=name.replace("ds_", "").replace("_semana", ""), source="weekly_direct"))
        elif name.startswith("ds_picking"):
            if frequency == "daily":
                direct = forecast.rename(columns={"prediction": "forecast"})
                direct["kpi"] = name.replace("ds_", "").replace("_dia", "")
                direct_picking_forecasts.append(direct[["fecha", "forecast", "kpi", "model_name"]])
                plot_history_and_forecast(history_df[["fecha", "target"]].tail(120), direct[["fecha", "forecast"]], name, PLOTS_DIR / f"forecast_{name}.png")
            else:
                weekly_forecasts.append(forecast.rename(columns={"prediction": "forecast"}).assign(kpi=name.replace("ds_", "").replace("_semana", ""), source="weekly_direct"))

    service_daily = pd.concat(service_daily_forecasts, ignore_index=True) if service_daily_forecasts else pd.DataFrame(columns=["fecha", "tipo_servicio", "forecast", "kpi", "model_name"])
    direct_picking = pd.concat(direct_picking_forecasts, ignore_index=True) if direct_picking_forecasts else pd.DataFrame(columns=["fecha", "forecast", "kpi", "model_name"])

    transformer = fit_transformer(context["joins"]["movimientos_albaranes"], context["albaranes"])
    if not transformer["global_curve"].empty:
        plot_transformer_curve(transformer["global_curve"], "Curva global D-3..D0", PLOTS_DIR / "transformer_curve_global.png")
        transformer["global_curve"].to_csv(FORECASTS_DIR / "transformer_curve_global.csv", index=False)
        transformer["intensity"].to_csv(FORECASTS_DIR / "transformer_intensity.csv", index=False)
        transformer["offset_probs"].to_csv(FORECASTS_DIR / "transformer_offsets.csv", index=False)

    transformed_lines = apply_transformer(service_daily[["fecha", "tipo_servicio", "forecast"]], transformer, metric="lines").assign(kpi="picking_lines_PI_transformer") if not service_daily.empty else pd.DataFrame()
    transformed_units = apply_transformer(service_daily[["fecha", "tipo_servicio", "forecast"]], transformer, metric="units").assign(kpi="picking_units_PI_transformer") if not service_daily.empty else pd.DataFrame()
    transformed = pd.concat([transformed_lines, transformed_units], ignore_index=True) if not transformed_lines.empty or not transformed_units.empty else pd.DataFrame(columns=["fecha", "tipo_servicio", "forecast", "kpi"])

    daily_frames = [
        frame[["fecha", "forecast", "kpi"]]
        for frame in [service_daily, direct_picking, transformed]
        if not frame.empty
    ]
    daily_output = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame(columns=["fecha", "forecast", "kpi"])
    daily_output.to_csv(FORECASTS_DIR / "daily_forecasts.csv", index=False)

    weekly_from_daily = reconcile_weekly_from_daily(daily_output) if not daily_output.empty else pd.DataFrame(columns=["fecha", "kpi", "forecast"])
    weekly_direct = pd.concat(weekly_forecasts, ignore_index=True) if weekly_forecasts else pd.DataFrame(columns=["fecha", "forecast", "kpi", "source"])
    weekly_output = pd.concat([weekly_from_daily.assign(source="daily_aggregated"), weekly_direct], ignore_index=True, sort=False)
    weekly_output.to_csv(FORECASTS_DIR / "weekly_forecasts.csv", index=False)
    return daily_output, weekly_output


def main(stage: str) -> None:
    settings, aliases, regex_rules = load_config()
    setup_logging(settings["logging"]["level"])
    ensure_dirs(INTERIM_DIR, PROCESSED_DIR, FEATURES_DIR, MODELS_DIR, BACKTESTS_DIR, FORECASTS_DIR, QA_DIR, REPORTS_DIR, PLOTS_DIR)

    if stage == "qa":
        run_qa(settings, aliases, regex_rules)
    elif stage == "features":
        run_features(settings, aliases, regex_rules)
    elif stage == "train":
        run_train(settings, aliases, regex_rules)
    elif stage == "backtest":
        run_backtest(settings, aliases, regex_rules)
    elif stage == "forecast":
        run_forecast(settings, aliases, regex_rules)
    elif stage == "all":
        run_qa(settings, aliases, regex_rules)
        run_features(settings, aliases, regex_rules)
        run_train(settings, aliases, regex_rules)
        run_backtest(settings, aliases, regex_rules)
        run_forecast(settings, aliases, regex_rules)
    else:
        raise ValueError(f"Unsupported stage: {stage}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Warehouse operational forecasting pipeline")
    parser.add_argument("--stage", default="all", choices=["qa", "features", "train", "backtest", "forecast", "all"])
    args = parser.parse_args()
    main(args.stage)
