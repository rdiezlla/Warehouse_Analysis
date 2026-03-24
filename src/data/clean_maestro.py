from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from src.utils.date_utils import safe_to_datetime
from src.utils.text_utils import canonicalize_columns, normalize_code
from src.utils.validation_utils import require_columns

LOGGER = logging.getLogger(__name__)


def clean_maestro(raw_df: pd.DataFrame, alias_map: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    df, _, missing = canonicalize_columns(raw_df, alias_map["maestro"])
    require_columns("maestro", missing)

    df["codigo"] = df.get("codigo", pd.Series(dtype=object)).map(normalize_code)
    df["creacion"], parse_failures = safe_to_datetime(df.get("creacion", pd.Series(dtype=object)), dayfirst=True)
    for column in ["kilos", "m2", "m3", "largo", "ancho", "alto"]:
        df[column] = pd.to_numeric(df.get(column, pd.Series(dtype=object)), errors="coerce")

    df["dimension_score"] = df[["kilos", "m3", "m2"]].fillna(0).sum(axis=1)
    df["duplicate_count"] = df.groupby("codigo")["codigo"].transform("size")
    dedup = (
        df.sort_values(["codigo", "dimension_score", "creacion"], ascending=[True, False, False])
        .dropna(subset=["codigo"])
        .drop_duplicates("codigo", keep="first")
        .reset_index(drop=True)
    )
    qa = {
        "rows": int(len(df)),
        "dedup_rows": int(len(dedup)),
        "duplicate_codes": int(df["codigo"].duplicated().sum()),
        "date_parse_failures": int(parse_failures),
    }
    LOGGER.info("Clean maestro complete with %s rows (%s dedup)", len(df), len(dedup))
    return df, dedup, qa
