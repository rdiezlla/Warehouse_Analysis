from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PLOTS_DIR = ROOT / "outputs" / "plots"
REPORTS_DIR = ROOT / "outputs" / "reports"


POWERBI_2026_SERIES = [
    {"anio_semana": "2026-1", "entregas": 2, "recogidas": 0},
    {"anio_semana": "2026-2", "entregas": 9, "recogidas": 0},
    {"anio_semana": "2026-3", "entregas": 28, "recogidas": 2},
    {"anio_semana": "2026-4", "entregas": 76, "recogidas": 11},
    {"anio_semana": "2026-5", "entregas": 61, "recogidas": 16},
    {"anio_semana": "2026-6", "entregas": 114, "recogidas": 12},
    {"anio_semana": "2026-7", "entregas": 111, "recogidas": 7},
    {"anio_semana": "2026-8", "entregas": 156, "recogidas": 7},
    {"anio_semana": "2026-9", "entregas": 90, "recogidas": 4},
    {"anio_semana": "2026-10", "entregas": 72, "recogidas": 20},
    {"anio_semana": "2026-11", "entregas": 84, "recogidas": 16},
    {"anio_semana": "2026-12", "entregas": 106, "recogidas": 23},
    {"anio_semana": "2026-13", "entregas": 94, "recogidas": 13},
    {"anio_semana": "2026-14", "entregas": 9, "recogidas": 4},
]


def add_point_labels(ax: plt.Axes, x_values: list[int], y_values: list[int], color: str, y_offset: int) -> None:
    for x_value, y_value in zip(x_values, y_values):
        if y_value <= 0:
            continue
        ax.annotate(
            f"{int(y_value)}",
            (x_value, y_value),
            textcoords="offset points",
            xytext=(0, y_offset),
            ha="center",
            fontsize=8,
            color=color,
        )


def build_plot(weekly: pd.DataFrame, summary: pd.DataFrame, output_path: Path) -> None:
    entregas_color = "#ef6c2f"
    recogidas_color = "#6f2dbd"
    total_color = "#4a4a4a"

    x_values = list(range(len(weekly)))
    entregas = weekly["entregas"].tolist()
    recogidas = weekly["recogidas"].tolist()

    fig, (ax_line, ax_bar) = plt.subplots(
        1,
        2,
        figsize=(18, 5.8),
        gridspec_kw={"width_ratios": [3.0, 1.0]},
        constrained_layout=True,
    )

    ax_line.plot(x_values, entregas, color=entregas_color, marker="o", linewidth=2.2, markersize=4, label="ENTREGAS")
    ax_line.plot(x_values, recogidas, color=recogidas_color, marker="o", linewidth=2.2, markersize=4, label="RECOGIDAS")
    add_point_labels(ax_line, x_values, entregas, entregas_color, 8)
    add_point_labels(ax_line, x_values, recogidas, recogidas_color, 8)

    ax_line.set_xticks(x_values, weekly["anio_semana"].tolist(), rotation=35, ha="right")
    ax_line.set_title("Volumen entregas/recogidas", fontsize=16, weight="bold")
    ax_line.set_xlabel("Ano - Semana")
    ax_line.set_ylabel("Albaranes")
    ax_line.grid(axis="y", linestyle=":", alpha=0.45)
    ax_line.spines[["top", "right", "left", "bottom"]].set_visible(False)
    ax_line.legend(frameon=False, loc="upper left", ncols=2)

    bars = ax_bar.bar(
        ["ENTREGAS", "RECOGIDAS", "TOTAL"],
        summary["valor"].tolist(),
        color=[entregas_color, recogidas_color, total_color],
        width=0.6,
    )
    ax_bar.set_title("Volumen entregas/recogidas", fontsize=16, weight="bold")
    ax_bar.set_ylabel("Albaranes")
    ax_bar.grid(axis="y", linestyle=":", alpha=0.45)
    ax_bar.spines[["top", "right", "left", "bottom"]].set_visible(False)

    for bar in bars:
        ax_bar.annotate(
            f"{int(bar.get_height())}",
            (bar.get_x() + bar.get_width() / 2, bar.get_height()),
            textcoords="offset points",
            xytext=(0, 6),
            ha="center",
            fontsize=10,
            color="#4a4a4a",
        )

    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    weekly = pd.DataFrame(POWERBI_2026_SERIES)
    weekly["total"] = weekly["entregas"] + weekly["recogidas"]

    summary = pd.DataFrame(
        [
            {"categoria": "ENTREGAS", "valor": int(weekly["entregas"].sum())},
            {"categoria": "RECOGIDAS", "valor": int(weekly["recogidas"].sum())},
            {"categoria": "TOTAL", "valor": int(weekly["total"].sum())},
        ]
    )

    weekly_path = REPORTS_DIR / "albaranes_powerbi_2026_semanal.csv"
    summary_path = REPORTS_DIR / "albaranes_powerbi_2026_resumen.csv"
    plot_path = PLOTS_DIR / "albaranes_powerbi_2026_entregas_recogidas.png"

    weekly.to_csv(weekly_path, index=False)
    summary.to_csv(summary_path, index=False)
    build_plot(weekly, summary, plot_path)

    print(f"CSV semanal: {weekly_path}")
    print(f"CSV resumen: {summary_path}")
    print(f"Grafico: {plot_path}")


if __name__ == "__main__":
    main()
