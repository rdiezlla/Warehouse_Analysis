from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def plot_backtest_errors(predictions: pd.DataFrame, title: str, output_path: Path) -> None:
    frame = predictions.copy()
    if "abs_error" not in frame.columns:
        frame["abs_error"] = (frame["y_pred"] - frame["y_true"]).abs()
    summary = frame.groupby(["fold_id", "model_name"], as_index=False)["abs_error"].mean()
    plt.figure(figsize=(12, 5))
    for model_name, subset in summary.groupby("model_name"):
        plt.plot(subset["fold_id"], subset["abs_error"], marker="o", label=model_name)
    plt.title(title)
    plt.xlabel("Fold")
    plt.ylabel("MAE medio")
    plt.legend()
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()
