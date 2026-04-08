from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "interim"
PLOTS_DIR = ROOT / "outputs" / "plots"
REPORTS_DIR = ROOT / "outputs" / "reports"

SOURCE_STEM = DATA_DIR / "albaranes_clean"
YEAR = 2024
VALID_CLASSES = ["entrega", "recogida"]
DEDUP_KEYS = ["codigo_servicio", "fecha_servicio", "clase_servicio"]


def load_albaranes() -> pd.DataFrame:
    parquet_path = SOURCE_STEM.with_suffix(".parquet")
    csv_path = SOURCE_STEM.with_suffix(".csv")

    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if csv_path.exists():
        return pd.read_csv(csv_path)
    raise FileNotFoundError(f"No se encontro {parquet_path} ni {csv_path}")


def build_weekly_series(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    work = df.copy()
    work["fecha_servicio"] = pd.to_datetime(work["fecha_servicio"], errors="coerce")
    work = work[work["fecha_servicio"].dt.year == YEAR].copy()
    work = work.dropna(subset=DEDUP_KEYS).drop_duplicates(subset=DEDUP_KEYS)

    unknown = work[~work["clase_servicio"].isin(VALID_CLASSES)].copy()
    known = work[work["clase_servicio"].isin(VALID_CLASSES)].copy()

    known["week_start"] = known["fecha_servicio"] - pd.to_timedelta(known["fecha_servicio"].dt.weekday, unit="D")
    weekly = (
        known.groupby(["week_start", "clase_servicio"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=VALID_CLASSES, fill_value=0)
        .reset_index()
        .sort_values("week_start")
    )

    weekly["semana_2024"] = range(1, len(weekly) + 1)
    weekly["anio_semana"] = weekly["semana_2024"].map(lambda value: f"{YEAR}-{value:02d}")
    weekly["entregas_unicas"] = weekly["entrega"].astype(int)
    weekly["recogidas_unicas"] = weekly["recogida"].astype(int)
    weekly["total_unicos"] = weekly["entregas_unicas"] + weekly["recogidas_unicas"]

    summary = pd.DataFrame(
        [
            {"categoria": "ENTREGAS", "albaranes_unicos": int(weekly["entregas_unicas"].sum())},
            {"categoria": "RECOGIDAS", "albaranes_unicos": int(weekly["recogidas_unicas"].sum())},
            {
                "categoria": "TOTAL",
                "albaranes_unicos": int(weekly["total_unicos"].sum()),
            },
            {
                "categoria": "SIN_CLASIFICAR_EXCLUIDOS",
                "albaranes_unicos": int(len(unknown)),
            },
        ]
    )

    weekly = weekly[
        [
            "week_start",
            "anio_semana",
            "entregas_unicas",
            "recogidas_unicas",
            "total_unicos",
        ]
    ]
    return weekly, summary


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
            fontsize=7,
            color=color,
        )


def build_plot(weekly: pd.DataFrame, summary: pd.DataFrame, output_path: Path) -> None:
    entregas_color = "#ef6c2f"
    recogidas_color = "#6f2dbd"

    x_values = list(range(len(weekly)))
    entregas = weekly["entregas_unicas"].tolist()
    recogidas = weekly["recogidas_unicas"].tolist()

    fig, (ax_line, ax_bar) = plt.subplots(
        1,
        2,
        figsize=(22, 7),
        gridspec_kw={"width_ratios": [3.4, 1.1]},
        constrained_layout=True,
    )

    ax_line.plot(x_values, entregas, color=entregas_color, marker="o", linewidth=2.2, markersize=4, label="ENTREGAS")
    ax_line.plot(x_values, recogidas, color=recogidas_color, marker="o", linewidth=2.2, markersize=4, label="RECOGIDAS")
    add_point_labels(ax_line, x_values, entregas, entregas_color, 8)
    add_point_labels(ax_line, x_values, recogidas, recogidas_color, 8)

    tick_step = max(1, len(weekly) // 14)
    tick_idx = x_values[::tick_step]
    if tick_idx[-1] != x_values[-1]:
        tick_idx.append(x_values[-1])
    tick_labels = weekly.iloc[tick_idx]["anio_semana"].tolist()
    ax_line.set_xticks(tick_idx, tick_labels, rotation=35, ha="right")
    ax_line.set_title("Volumen entregas/recogidas", fontsize=14, weight="bold")
    ax_line.set_xlabel("Ano - Semana")
    ax_line.set_ylabel("Albaranes unicos")
    ax_line.grid(axis="y", linestyle=":", alpha=0.45)
    ax_line.spines[["top", "right", "left", "bottom"]].set_visible(False)
    ax_line.legend(frameon=False, loc="upper left", ncols=2)

    total_entregas = int(summary.loc[summary["categoria"] == "ENTREGAS", "albaranes_unicos"].iloc[0])
    total_recogidas = int(summary.loc[summary["categoria"] == "RECOGIDAS", "albaranes_unicos"].iloc[0])
    total_global = int(summary.loc[summary["categoria"] == "TOTAL", "albaranes_unicos"].iloc[0])
    bars = ax_bar.bar(
        ["ENTREGAS", "RECOGIDAS", "TOTAL"],
        [total_entregas, total_recogidas, total_global],
        color=[entregas_color, recogidas_color, "#4a4a4a"],
        width=0.6,
    )
    ax_bar.set_title("Volumen entregas/recogidas", fontsize=14, weight="bold")
    ax_bar.grid(axis="y", linestyle=":", alpha=0.45)
    ax_bar.spines[["top", "right", "left", "bottom"]].set_visible(False)
    ax_bar.set_ylabel("Albaranes unicos")
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

    albaranes = load_albaranes()
    weekly, summary = build_weekly_series(albaranes)

    weekly_path = REPORTS_DIR / "albaranes_unicos_2024_semanal.csv"
    summary_path = REPORTS_DIR / "albaranes_unicos_2024_resumen.csv"
    plot_path = PLOTS_DIR / "albaranes_unicos_2024_entregas_recogidas.png"

    weekly.to_csv(weekly_path, index=False)
    summary.to_csv(summary_path, index=False)
    build_plot(weekly, summary, plot_path)

    total_entregas = int(summary.loc[summary["categoria"] == "ENTREGAS", "albaranes_unicos"].iloc[0])
    total_recogidas = int(summary.loc[summary["categoria"] == "RECOGIDAS", "albaranes_unicos"].iloc[0])
    total = int(summary.loc[summary["categoria"] == "TOTAL", "albaranes_unicos"].iloc[0])
    excluded = int(summary.loc[summary["categoria"] == "SIN_CLASIFICAR_EXCLUIDOS", "albaranes_unicos"].iloc[0])

    print(f"Entregas unicas {YEAR}: {total_entregas}")
    print(f"Recogidas unicas {YEAR}: {total_recogidas}")
    print(f"Total unico {YEAR}: {total}")
    print(f"Sin clasificar excluidos {YEAR}: {excluded}")
    print(f"CSV semanal: {weekly_path}")
    print(f"CSV resumen: {summary_path}")
    print(f"Grafico: {plot_path}")


if __name__ == "__main__":
    main()
