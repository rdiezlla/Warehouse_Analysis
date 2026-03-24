from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def plot_history_and_forecast(history: pd.DataFrame, forecast: pd.DataFrame, title: str, output_path: Path) -> None:
    plt.figure(figsize=(12, 5))
    plt.plot(pd.to_datetime(history["fecha"]), history["target"], label="historico", linewidth=1.5)
    plt.plot(pd.to_datetime(forecast["fecha"]), forecast["forecast"], label="forecast", linewidth=2)
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()


def plot_transformer_curve(curve: pd.DataFrame, title: str, output_path: Path) -> None:
    plt.figure(figsize=(8, 4))
    plt.bar(curve["offset_dias"], curve["prob_offset"])
    plt.title(title)
    plt.xlabel("Offset D")
    plt.ylabel("Probabilidad")
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()
