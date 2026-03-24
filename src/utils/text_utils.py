import re
import unicodedata
from typing import Iterable

import numpy as np
import pandas as pd


def normalize_column_name(value: str) -> str:
    value = str(value).strip().lower()
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def normalize_code(value: object) -> object:
    if pd.isna(value):
        return np.nan
    value = str(value).strip().upper()
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"\s+", " ", value)
    if re.fullmatch(r"\d+\.0", value):
        value = value[:-2]
    if re.fullmatch(r"\d+", value):
        value = str(int(value))
    return value


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    value = str(value).strip().upper()
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"\s+", " ", value)
    return value


def canonicalize_columns(df: pd.DataFrame, alias_map: dict[str, Iterable[str]]) -> tuple[pd.DataFrame, dict[str, str], list[str]]:
    normalized = {col: normalize_column_name(col) for col in df.columns}
    df = df.rename(columns=normalized)
    reverse_lookup = {normalize_column_name(col): col for col in df.columns}
    rename_map: dict[str, str] = {}
    missing: list[str] = []
    for canonical, aliases in alias_map.items():
        candidates = [normalize_column_name(canonical), *[normalize_column_name(alias) for alias in aliases]]
        match = next((candidate for candidate in candidates if candidate in df.columns), None)
        if match is None:
            missing.append(canonical)
            continue
        rename_map[match] = canonical
    df = df.rename(columns=rename_map)
    return df, rename_map, missing
