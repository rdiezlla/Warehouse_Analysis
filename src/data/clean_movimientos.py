from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from src.utils.date_utils import safe_to_datetime
from src.utils.text_utils import canonicalize_columns, normalize_code, normalize_text
from src.utils.validation_utils import require_columns

LOGGER = logging.getLogger(__name__)


def clean_movimientos(raw_df: pd.DataFrame, alias_map: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    df, _, missing = canonicalize_columns(raw_df, alias_map["movimientos"])
    require_columns("movimientos", missing)

    df["fecha_inicio"], start_failures = safe_to_datetime(df.get("fecha_inicio", pd.Series(dtype=object)), dayfirst=True)
    df["fecha_finalizacion"], end_failures = safe_to_datetime(df.get("fecha_finalizacion", pd.Series(dtype=object)), dayfirst=True)
    df["fecha_operativa_mov"] = df["fecha_finalizacion"].dt.normalize()
    fallback_mask = df["fecha_operativa_mov"].isna() & df["fecha_inicio"].notna()
    df.loc[fallback_mask, "fecha_operativa_mov"] = df.loc[fallback_mask, "fecha_inicio"].dt.normalize()
    df["flag_fecha_operativa_fallback_inicio"] = fallback_mask.astype(int)

    df["tipo_movimiento"] = df.get("tipo_movimiento", pd.Series(dtype=object)).map(normalize_text)
    df["codigo_articulo"] = df.get("articulo", pd.Series(dtype=object)).map(normalize_code)
    df["pedido_externo"] = df.get("pedido_externo", pd.Series(dtype=object)).map(normalize_code)
    df["cantidad"] = pd.to_numeric(df.get("cantidad", pd.Series(dtype=object)), errors="coerce")
    df["duracion_segundos"] = (df["fecha_finalizacion"] - df["fecha_inicio"]).dt.total_seconds()
    df["denominacion_articulo"] = df.get("denominacion_articulo", pd.Series(dtype=object)).map(normalize_text)

    pi_mask = df["tipo_movimiento"].eq("PI")
    qa = {
        "rows": int(len(df)),
        "date_parse_failures_inicio": int(start_failures),
        "date_parse_failures_finalizacion": int(end_failures),
        "missing_cantidad_pct": float(df["cantidad"].isna().mean()),
        "missing_fecha_operativa_pct": float(df["fecha_operativa_mov"].isna().mean()),
        "movement_distribution": df["tipo_movimiento"].value_counts(dropna=False).head(20).to_dict(),
        "pi_rows": int(pi_mask.sum()),
        "pi_missing_pedido_externo_pct": float(df.loc[pi_mask, "pedido_externo"].isna().mean()) if pi_mask.any() else 1.0,
    }
    LOGGER.info("Clean movimientos complete with %s rows", len(df))
    return df, qa
