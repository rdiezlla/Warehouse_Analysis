from __future__ import annotations

import logging

import pandas as pd

LOGGER = logging.getLogger(__name__)


def warn_missing_columns(dataset_name: str, frame: pd.DataFrame, required_columns: list[str]) -> list[str]:
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        message = f"{dataset_name}: faltan columnas normalizadas {missing}"
        LOGGER.warning(message)
        return [message]
    return []


def validate_key_nulls(dataset_name: str, frame: pd.DataFrame, key_columns: list[str]) -> list[str]:
    warnings: list[str] = []
    row_count = max(len(frame), 1)
    for column in key_columns:
        if column not in frame.columns:
            continue
        null_pct = float(frame[column].isna().sum() / row_count)
        if null_pct > 0:
            message = f"{dataset_name}: {column} tiene {null_pct:.2%} nulos"
            LOGGER.warning(message)
            warnings.append(message)
    return warnings
