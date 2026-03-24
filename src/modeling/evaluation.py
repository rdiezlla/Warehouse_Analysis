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
        "wape": float(abs_error.sum() / denom) if denom else 0.0,
        "bias": float(error.mean()),
        "mae_peak": float(abs_error.to_numpy()[peak_mask].mean()) if peak_mask.any() else 0.0,
        "wape_peak": float(abs_error.to_numpy()[peak_mask].sum() / np.abs(actual.to_numpy()[peak_mask].astype(float)).sum()) if peak_mask.any() and np.abs(actual.to_numpy()[peak_mask].astype(float)).sum() else 0.0,
    }
