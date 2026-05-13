from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from src.data.service_classifier import classify_dataframe
from src.data.service_date_utils import build_service_date_quality_qa, resolve_service_target_date
from src.utils.date_utils import safe_to_datetime
from src.utils.text_utils import canonicalize_columns, normalize_code
from src.utils.validation_utils import require_columns

LOGGER = logging.getLogger(__name__)

DATE_COLUMNS = [
    "fecha_servicio",
    "inicio_evento",
    "creacion_solicitud",
    "borrado_solicitud",
    "modificacion_linea",
    "fin_evento",
    "reservation_start_date",
    "reservation_finish_date",
    "borrado_linea",
    "creacion_pedido",
    "ultima_modificacion",
]

SOLICITUD_PREFERRED_DATE_FIELDS = {
    "delivery": ["fecha_servicio", "fecha_inicio_evento", "reservation_start_date"],
    "pickup": ["fecha_servicio", "fecha_fin_evento", "reservation_finish_date"],
}

SOLICITUD_FALLBACK_DATE_FIELDS = {
    "delivery": ["fecha_fin_evento", "reservation_finish_date"],
    "pickup": ["fecha_inicio_evento", "reservation_start_date"],
}


def clean_solicitudes(raw_df: pd.DataFrame, alias_map: dict[str, Any], regex_rules: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    df, _, missing = canonicalize_columns(raw_df, alias_map["solicitudes"])
    require_columns("solicitudes", missing)

    parse_failures: dict[str, int] = {}
    for column in DATE_COLUMNS:
        if column in df.columns:
            df[column], failures = safe_to_datetime(df[column], dayfirst=True)
            parse_failures[column] = failures
        else:
            df[column] = pd.NaT
            parse_failures[column] = 0

    for column in ["cant_solicitada", "cant_confirmada", "cant_almacenada"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        else:
            df[column] = np.nan

    df["codigo_generico"] = df.get("codigo_generico", pd.Series(dtype=object)).map(normalize_code)
    df["codigo_articulo"] = df.get("articulo", pd.Series(dtype=object)).map(normalize_code)
    df["codigo_servicio"] = df["codigo_generico"]
    df = classify_dataframe(df, regex_rules)
    df["linea_solicitada"] = 1
    df["fecha_creacion"] = df["creacion_solicitud"].dt.normalize()
    df["fecha_servicio"] = df["fecha_servicio"].dt.normalize()
    df["fecha_inicio_evento"] = df["inicio_evento"].dt.normalize()
    df["fecha_inicio_evento"] = df["fecha_inicio_evento"].fillna(df["fecha_servicio"])
    df["fecha_fin_evento"] = df["fin_evento"].dt.normalize()
    df = pd.concat(
        [
            df,
            resolve_service_target_date(
                df,
                preferred_fields_by_bucket=SOLICITUD_PREFERRED_DATE_FIELDS,
                fallback_fields_by_bucket=SOLICITUD_FALLBACK_DATE_FIELDS,
            ),
        ],
        axis=1,
    )
    df["lead_time_dias"] = (df["fecha_servicio_objetiva"] - df["fecha_creacion"]).dt.days

    qa = {
        "rows": int(len(df)),
        "date_parse_failures": parse_failures,
        "null_codigo_generico_pct": float(df["codigo_generico"].isna().mean()),
        "service_distribution": df["tipo_servicio"].value_counts(dropna=False).to_dict(),
        "service_date_logic": build_service_date_quality_qa(df),
    }
    LOGGER.info("Clean solicitudes complete with %s rows", len(df))
    return df, qa
