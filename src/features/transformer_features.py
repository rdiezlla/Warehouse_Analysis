from __future__ import annotations

import pandas as pd


def build_transformer_training_table(movimientos_albaranes: pd.DataFrame) -> pd.DataFrame:
    matched = movimientos_albaranes[(movimientos_albaranes["tipo_movimiento"] == "PI") & (movimientos_albaranes["match_albaranes"])].copy()
    service_date_col = "fecha_servicio_objetiva" if "fecha_servicio_objetiva" in matched.columns else "fecha_servicio"
    matched = matched.dropna(subset=["fecha_operativa_mov", service_date_col])
    matched["service_date"] = pd.to_datetime(matched[service_date_col]).dt.normalize()
    matched["picking_date"] = matched["fecha_operativa_mov"].dt.normalize()
    matched["offset_dias"] = (matched["service_date"] - matched["picking_date"]).dt.days
    return matched[matched["offset_dias"].between(0, 3)]
