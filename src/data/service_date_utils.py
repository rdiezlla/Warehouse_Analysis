from __future__ import annotations

from typing import Iterable

import pandas as pd

DELIVERY_SERVICE_TYPES = {"SGE", "SGP"}
PICKUP_SERVICE_TYPES = {"EGE"}
DEFAULT_VERSION_TS_COLUMNS = [
    "ultima_modificacion",
    "modificacion_linea",
    "creacion_pedido",
    "creacion_solicitud",
    "fecha_creacion",
]


def service_date_bucket(row: pd.Series) -> str:
    tipo_servicio = str(row.get("tipo_servicio", "") or "")
    clase_servicio = str(row.get("clase_servicio", "") or "")
    if tipo_servicio in PICKUP_SERVICE_TYPES or clase_servicio == "recogida":
        return "pickup"
    return "delivery"


def resolve_service_target_date(
    df: pd.DataFrame,
    *,
    preferred_fields_by_bucket: dict[str, list[str]],
    fallback_fields_by_bucket: dict[str, list[str]],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, row in df.iterrows():
        bucket = service_date_bucket(row)
        preferred_fields = preferred_fields_by_bucket.get(bucket, [])
        fallback_fields = fallback_fields_by_bucket.get(bucket, [])
        ordered_fields = preferred_fields + fallback_fields
        selected_field = None
        selected_value = pd.NaT
        selected_rank = 999
        is_fallback = 0

        for rank, field in enumerate(ordered_fields, start=1):
            value = row.get(field, pd.NaT)
            if pd.notna(value):
                selected_field = field
                selected_value = pd.Timestamp(value).normalize()
                selected_rank = rank
                is_fallback = int(field in fallback_fields)
                break

        rows.append(
            {
                "fecha_servicio_objetiva": selected_value,
                "campo_fecha_objetiva_usado": selected_field if selected_field is not None else "<MISSING>",
                "flag_fecha_objetiva_fallback": is_fallback,
                "flag_fecha_objetiva_missing": int(selected_field is None),
                "service_date_priority_rank": selected_rank,
                "service_date_bucket": bucket,
            }
        )
    return pd.DataFrame(rows, index=df.index)


def build_service_date_quality_qa(df: pd.DataFrame) -> dict[str, object]:
    if df.empty or "fecha_servicio_objetiva" not in df.columns:
        return {
            "rows": 0,
            "pct_missing": 0.0,
            "pct_fallback": 0.0,
            "missing_by_service": {},
            "fallback_by_service": {},
            "field_usage_by_service": {},
        }

    missing_by_service = (
        df.groupby("tipo_servicio")["flag_fecha_objetiva_missing"]
        .mean()
        .fillna(0.0)
        .to_dict()
    )
    fallback_by_service = (
        df.groupby("tipo_servicio")["flag_fecha_objetiva_fallback"]
        .mean()
        .fillna(0.0)
        .to_dict()
    )
    field_usage_by_service = (
        df.groupby(["tipo_servicio", "campo_fecha_objetiva_usado"])
        .size()
        .reset_index(name="n_registros")
    )
    usage_payload: dict[str, dict[str, int]] = {}
    for tipo_servicio, group in field_usage_by_service.groupby("tipo_servicio"):
        usage_payload[str(tipo_servicio)] = {
            str(row["campo_fecha_objetiva_usado"]): int(row["n_registros"])
            for _, row in group.iterrows()
        }

    return {
        "rows": int(len(df)),
        "pct_missing": float(df["flag_fecha_objetiva_missing"].mean()),
        "pct_fallback": float(df["flag_fecha_objetiva_fallback"].mean()),
        "missing_by_service": missing_by_service,
        "fallback_by_service": fallback_by_service,
        "field_usage_by_service": usage_payload,
    }


def get_service_target_date_column(df: pd.DataFrame, fallback: str) -> str:
    return "fecha_servicio_objetiva" if "fecha_servicio_objetiva" in df.columns else fallback


def build_request_level_service_dates(
    df: pd.DataFrame,
    *,
    key_col: str = "codigo_generico",
    version_columns: Iterable[str] = DEFAULT_VERSION_TS_COLUMNS,
) -> pd.DataFrame:
    target_col = get_service_target_date_column(df, "fecha_inicio_evento")
    working = df.copy()
    if "campo_fecha_objetiva_usado" not in working.columns:
        working["campo_fecha_objetiva_usado"] = target_col
    if "flag_fecha_objetiva_fallback" not in working.columns:
        working["flag_fecha_objetiva_fallback"] = 0
    if "flag_fecha_objetiva_missing" not in working.columns:
        working["flag_fecha_objetiva_missing"] = working[target_col].isna().astype(int)
    if "service_date_priority_rank" not in working.columns:
        working["service_date_priority_rank"] = working["flag_fecha_objetiva_missing"].map({0: 1, 1: 999})

    version_candidates = []
    for column in version_columns:
        if column in working.columns:
            version_candidates.append(pd.to_datetime(working[column], errors="coerce"))
    if version_candidates:
        working["service_date_version_ts"] = pd.concat(version_candidates, axis=1).max(axis=1)
    else:
        working["service_date_version_ts"] = pd.NaT
    working["service_date_version_ts"] = working["service_date_version_ts"].fillna(pd.Timestamp("1900-01-01"))

    selection_columns = [
        key_col,
        target_col,
        "tipo_servicio",
        "clase_servicio",
        "urgencia_norm",
        "campo_fecha_objetiva_usado",
        "flag_fecha_objetiva_fallback",
        "flag_fecha_objetiva_missing",
        "service_date_priority_rank",
    ]
    for column in ["fecha_inicio_evento", "fecha_fin_evento"]:
        if column in working.columns and column not in selection_columns:
            selection_columns.append(column)
    selected = (
        working[selection_columns + ["service_date_version_ts"]]
        .sort_values(
            [
                key_col,
                "flag_fecha_objetiva_missing",
                "service_date_priority_rank",
                "service_date_version_ts",
                target_col,
            ],
            ascending=[True, True, True, False, False],
            na_position="last",
        )
        .groupby(key_col, as_index=False)
        .head(1)
        .copy()
    )
    selected = selected.rename(columns={target_col: "fecha_servicio_objetiva"})
    return selected.drop(columns=["service_date_version_ts"], errors="ignore")
