from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.utils.io_utils import ensure_dirs, save_dataframe


def _write_dashboard_json(frame: pd.DataFrame, path: Path, limit: int = 5000) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.head(limit).to_json(path, orient="records", force_ascii=False, indent=2, date_format="iso")


def write_market_basket_outputs(pairs: pd.DataFrame, rules: pd.DataFrame, output_dir: Path, dashboard_dir: Path) -> dict[str, Path]:
    ensure_dirs(output_dir, dashboard_dir)
    save_dataframe(pairs, output_dir / "pares_frecuentes", index=False)
    save_dataframe(rules, output_dir / "reglas_asociacion", index=False)
    _write_dashboard_json(pairs, dashboard_dir / "pares_frecuentes.json")
    _write_dashboard_json(rules, dashboard_dir / "reglas_asociacion.json")
    return {
        "pairs_csv": output_dir / "pares_frecuentes.csv",
        "pairs_parquet": output_dir / "pares_frecuentes.parquet",
        "rules_csv": output_dir / "reglas_asociacion.csv",
        "rules_parquet": output_dir / "reglas_asociacion.parquet",
        "pairs_json": dashboard_dir / "pares_frecuentes.json",
        "rules_json": dashboard_dir / "reglas_asociacion.json",
    }
