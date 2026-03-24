import json
import logging
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

LOGGER = logging.getLogger(__name__)


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False, default=str)


def copy_if_needed(src: Path, dst: Path) -> Path:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists() or src.stat().st_mtime > dst.stat().st_mtime:
        shutil.copy2(src, dst)
        LOGGER.info("Copied source file %s -> %s", src.name, dst)
    return dst


def save_dataframe(df: pd.DataFrame, path_without_suffix: Path, index: bool = False) -> None:
    path_without_suffix.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path_without_suffix.with_suffix(".csv"), index=index)
    save_parquet_safe(df, path_without_suffix.with_suffix(".parquet"), index=index)


def save_parquet_safe(df: pd.DataFrame, path: Path, index: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    parquet_df = df.copy()
    object_columns = parquet_df.select_dtypes(include=["object"]).columns
    for column in object_columns:
        parquet_df[column] = parquet_df[column].map(lambda value: value if pd.isna(value) else str(value))
    parquet_df.to_parquet(path, index=index)


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported table extension: {path}")
