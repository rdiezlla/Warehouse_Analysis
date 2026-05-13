from __future__ import annotations

import numpy as np
import pandas as pd


def compute_metrics(actual: pd.Series, prediction: pd.Series) -> dict[str, float]:
    actual = pd.Series(actual).astype(float).reset_index(drop=True)
    prediction = pd.Series(prediction).astype(float).reset_index(drop=True)
    error = prediction - actual
    abs_error = error.abs()
    denom = actual.abs().sum()
    top_threshold = actual.quantile(0.9) if not actual.empty else 0.0
    peak_mask = (actual >= top_threshold).to_numpy()
    return {
        "mae": float(abs_error.mean()),
        "rmse": float(np.sqrt((error**2).mean())),
        "wape": float(abs_error.sum() / denom) if denom else np.nan,
        "bias": float(error.mean()),
        "mae_peak": float(abs_error.to_numpy()[peak_mask].mean()) if peak_mask.any() else np.nan,
        "wape_peak": float(abs_error.to_numpy()[peak_mask].sum() / np.abs(actual.to_numpy()[peak_mask].astype(float)).sum()) if peak_mask.any() and np.abs(actual.to_numpy()[peak_mask].astype(float)).sum() else np.nan,
    }


def build_oof_frame(dataset_name: str, fold_id: int, model_name: str, fecha: pd.Series, y_true: pd.Series, y_pred: pd.Series) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "dataset_name": dataset_name,
            "fold_id": fold_id,
            "model_name": model_name,
            "fecha": pd.to_datetime(fecha),
            "y_true": pd.Series(y_true).astype(float).reset_index(drop=True),
            "y_pred": pd.Series(y_pred).astype(float).reset_index(drop=True),
        }
    )
    frame["abs_error"] = (frame["y_pred"] - frame["y_true"]).abs()
    if frame["y_true"].abs().sum() == 0:
        frame["is_peak"] = 0
    else:
        peak_threshold = frame["y_true"].quantile(0.9)
        frame["is_peak"] = (frame["y_true"] >= peak_threshold).astype(int)
    frame["evaluation_view"] = frame["fecha"].dt.year.map(lambda year: "ranking_operativo_principal" if year <= 2024 else "stress_test_2025_2026")
    return frame


def compute_metrics_by_fold(oof_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    grouped = oof_df.groupby(["dataset_name", "fold_id", "model_name"])
    for (dataset_name, fold_id, model_name), group in grouped:
        rows.append(
            {
                "dataset_name": dataset_name,
                "fold_id": fold_id,
                "model_name": model_name,
                "evaluation_view": "stress_test_2025_2026" if (group["evaluation_view"] == "stress_test_2025_2026").any() else "ranking_operativo_principal",
                **compute_metrics(group["y_true"], group["y_pred"]),
            }
        )
    return pd.DataFrame(rows)


def summarize_oof_predictions(oof_df: pd.DataFrame, evaluation_view: str | None = None) -> pd.DataFrame:
    scoped_oof = oof_df[oof_df["evaluation_view"] == evaluation_view].copy() if evaluation_view else oof_df.copy()
    fold_metrics = compute_metrics_by_fold(scoped_oof)
    if evaluation_view:
        fold_metrics = fold_metrics[fold_metrics["evaluation_view"] == evaluation_view].copy()
    summary = (
        fold_metrics.groupby(["dataset_name", "model_name"])
        .agg(
            mean_mae=("mae", "mean"),
            median_mae=("mae", "median"),
            std_mae=("mae", "std"),
            mean_wape=("wape", "mean"),
            mean_bias=("bias", "mean"),
        )
        .reset_index()
    )
    p90_abs_error = scoped_oof.groupby(["dataset_name", "model_name"])["abs_error"].quantile(0.90).reset_index(name="p90_abs_error")

    peak_rows = []
    for (dataset_name, model_name), group in scoped_oof.groupby(["dataset_name", "model_name"]):
        peak_group = group[group["is_peak"] == 1]
        peak_metrics = compute_metrics(peak_group["y_true"], peak_group["y_pred"]) if not peak_group.empty else {"mae": np.nan, "wape": np.nan}
        peak_rows.append(
            {
                "dataset_name": dataset_name,
                "model_name": model_name,
                "mae_peak": peak_metrics["mae"],
                "wape_peak": peak_metrics["wape"],
            }
        )
    peak_summary = pd.DataFrame(peak_rows)
    result = summary.merge(p90_abs_error, on=["dataset_name", "model_name"], how="left").merge(peak_summary, on=["dataset_name", "model_name"], how="left")
    if evaluation_view:
        result["evaluation_view"] = evaluation_view
    return result


def rank_models(summary_df: pd.DataFrame) -> pd.DataFrame:
    ranked = summary_df.sort_values(["dataset_name", "mean_wape", "mean_mae", "p90_abs_error"], na_position="last").copy()
    ranked["rank"] = ranked.groupby("dataset_name").cumcount() + 1
    return ranked
