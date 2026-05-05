from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd

from src.utils.text_utils import normalize_text


def _class_from_action(value: object) -> str | None:
    action = normalize_text(value)
    if action in {"ENVIO", "ENTREGA"}:
        return "entrega"
    if action == "RECOGIDA":
        return "recogida"
    return None


def _service_type_from_code(codigo: str) -> str | None:
    if codigo.startswith("SGP"):
        return "SGP"
    if codigo.startswith("SGE") or codigo.startswith("SG"):
        return "SGE"
    if codigo.startswith("EGE") or codigo.startswith("EG") or codigo.startswith("DEGE"):
        return "EGE"
    return None


def classify_servicio(row: pd.Series, regex_rules: dict[str, Any]) -> tuple[str, str, str]:
    codigo = normalize_text(row.get("codigo_servicio", ""))
    action_class = _class_from_action(row.get("accion", ""))
    concepto = normalize_text(row.get("concepto_denominacion_evento_asociado", row.get("solicitud", "")))

    service_type = _service_type_from_code(codigo)
    if service_type is not None:
        default_class = "recogida" if service_type == "EGE" else "entrega"
        if action_class is not None:
            motivo = "por_prefijo_y_accion" if action_class == default_class else "por_accion"
            return service_type, action_class, motivo
        return service_type, default_class, "por_prefijo"

    if action_class is not None:
        return "UNK", action_class, "por_accion"

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
