from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd

from src.utils.text_utils import normalize_text


def classify_servicio(row: pd.Series, regex_rules: dict[str, Any]) -> tuple[str, str, str]:
    codigo = normalize_text(row.get("codigo_servicio", ""))
    concepto = normalize_text(row.get("concepto_denominacion_evento_asociado", row.get("solicitud", "")))

    if codigo.startswith("SGE"):
        return "SGE", "entrega", "por_prefijo"
    if codigo.startswith("SGP"):
        return "SGP", "entrega", "por_prefijo"
    if codigo.startswith("EGE"):
        return "EGE", "recogida", "por_prefijo"

    recogida_regex = "|".join(regex_rules["service_classifier"]["recogida_patterns"])
    entrega_regex = "|".join(regex_rules["service_classifier"]["entrega_patterns"])

    if re.search(recogida_regex, concepto):
        return "UNK", "recogida", "por_concepto"
    if re.search(entrega_regex, concepto):
        return "UNK", "entrega", "por_concepto"
    return "UNK", "UNK", "unk"


def classify_dataframe(df: pd.DataFrame, regex_rules: dict[str, Any]) -> pd.DataFrame:
    classified = df.apply(lambda row: classify_servicio(row, regex_rules), axis=1, result_type="expand")
    classified.columns = ["tipo_servicio", "clase_servicio", "motivo_clasificacion"]
    return pd.concat([df, classified], axis=1)


def build_service_classification_qa(df: pd.DataFrame) -> dict[str, Any]:
    unk_mask = df["clase_servicio"].eq("UNK")
    examples = (
        df.groupby("clase_servicio")[["codigo_servicio", "concepto_denominacion_evento_asociado"]]
        .head(5)
        .to_dict(orient="records")
    )
    return {
        "pct_unk": float(unk_mask.mean()),
        "top_unknown_patterns": df.loc[unk_mask, "concepto_denominacion_evento_asociado"].fillna("<EMPTY>").value_counts().head(50).to_dict(),
        "examples": examples,
    }
