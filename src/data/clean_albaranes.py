from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from src.data.service_classifier import build_service_classification_qa, classify_dataframe
from src.data.service_date_utils import build_service_date_quality_qa, resolve_service_target_date
from src.utils.date_utils import safe_to_datetime
from src.utils.text_utils import canonicalize_columns, normalize_code, normalize_text
from src.utils.validation_utils import require_columns

LOGGER = logging.getLogger(__name__)

NUMERIC_COLUMNS = [
    "pales_in",
    "cajas_in",
    "m3_in",
    "pales_out",
    "m3_out",
    "cajas_out",
    "peso_kg_raw",
    "volumen",
]

ALBARAN_OPTIONAL_DATE_COLUMNS = [
    "fecha_servicio",
    "fecha_inicio_evento",
    "inicio_evento",
    "fecha_entrega",
    "reservation_start_date",
    "fecha_fin_evento",
    "fin_evento",
    "fecha_recogida",
    "reservation_finish_date",
]

ALBARAN_PREFERRED_DATE_FIELDS = {
    "delivery": [
        "fecha_inicio_evento",
        "inicio_evento",
        "fecha_entrega",
        "reservation_start_date",
    ],
    "pickup": [
        "fecha_fin_evento",
        "fin_evento",
        "fecha_recogida",
        "reservation_finish_date",
    ],
}

ALBARAN_FALLBACK_DATE_FIELDS = {
    "delivery": ["fecha_servicio"],
    "pickup": ["fecha_servicio"],
}


def normalize_urgencia(series: pd.Series, regex_rules: dict[str, Any]) -> pd.DataFrame:
    mapping = {}
    for canonical, values in regex_rules["urgency_mapping"].items():
        for value in values:
            mapping[normalize_text(value)] = canonical
    normalized = series.map(lambda value: mapping.get(normalize_text(value), "UNKNOWN"))
    return pd.DataFrame(
        {
            "urgencia_norm": normalized,
            "flag_urgencia_missing": series.isna().astype(int),
            "flag_urgencia_original_inconsistente": (~series.isna() & normalized.eq("UNKNOWN")).astype(int),
        }
    )


def clean_albaranes(raw_df: pd.DataFrame, alias_map: dict[str, Any], regex_rules: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    df, _, missing = canonicalize_columns(raw_df, alias_map["albaranes"])
    require_columns("albaranes", missing)

    parse_failures_by_column: dict[str, int] = {}
    for column in ALBARAN_OPTIONAL_DATE_COLUMNS:
        if column in df.columns:
            df[column], failures = safe_to_datetime(df[column], dayfirst=True)
            parse_failures_by_column[column] = failures
        else:
            df[column] = pd.NaT
            parse_failures_by_column[column] = 0

    df["codigo_servicio"] = df.get("descripcion", pd.Series(dtype=object)).map(normalize_code)
    df["concepto_denominacion_evento_asociado"] = df.get("concepto_denominacion_evento_asociado", pd.Series(dtype=object)).map(normalize_text)
    df = classify_dataframe(df, regex_rules)
    df = pd.concat(
        [
            df,
            resolve_service_target_date(
                df,
                preferred_fields_by_bucket=ALBARAN_PREFERRED_DATE_FIELDS,
                fallback_fields_by_bucket=ALBARAN_FALLBACK_DATE_FIELDS,
            ),
        ],
        axis=1,
    )
    df = pd.concat([df, normalize_urgencia(df.get("urgencia", pd.Series(dtype=object)), regex_rules)], axis=1)

    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        else:
            df[column] = np.nan

    df["peso_kg"] = df["peso_kg_raw"]
    df["vol_m3"] = df["volumen"]
    df["kg_volumetrico"] = df["vol_m3"] * 270.0
    df["kg_facturable"] = np.ceil(np.nanmax(np.vstack([df["peso_kg"].fillna(0).to_numpy(), df["kg_volumetrico"].fillna(0).to_numpy()]), axis=0))

    qa = {
        "rows": int(len(df)),
        "date_parse_failures": parse_failures_by_column,
        "date_parse_failure_pct": {
            column: float(failures / max(len(df), 1))
            for column, failures in parse_failures_by_column.items()
        },
        "null_codigo_servicio_pct": float(df["codigo_servicio"].isna().mean()),
        "service_classification": build_service_classification_qa(df),
        "service_date_logic": build_service_date_quality_qa(df),
        "urgencia_distribution": df["urgencia_norm"].value_counts(dropna=False).to_dict(),
    }
    LOGGER.info("Clean albaranes complete with %s rows", len(df))
    return df, qa
