from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.pipelines.common.config import PipelinePaths, get_paths, load_column_aliases
from src.pipelines.common.load_inputs import LoadedInputs, load_inputs
from src.pipelines.common.schemas import (
    ALBARANES_COLUMNS,
    ARTICULOS_COLUMNS,
    LINEAS_COLUMNS,
    MOVIMIENTOS_COLUMNS,
    NORMALIZED_FILES,
    STOCK_COLUMNS,
)
from src.utils.io_utils import ensure_dirs, save_parquet_safe, write_json
from src.utils.text_utils import canonicalize_columns, normalize_code, normalize_column_name, normalize_text

LOGGER = logging.getLogger(__name__)


def _coerce_datetime(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="datetime64[ns]")
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def _coerce_number(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="float64")
    return pd.to_numeric(series, errors="coerce")


def _string_code(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="object")
    return series.map(normalize_code).astype("string")


def _text(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="object")
    return series.map(lambda value: normalize_text(value) if pd.notna(value) else np.nan).astype("string")


def _service_flow_from_action(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="object")

    normalized = series.map(lambda value: normalize_text(value) if pd.notna(value) else "")
    return normalized.map(
        lambda value: "envio"
        if value in {"ENVIO", "ENTREGA"}
        else "recogida"
        if value == "RECOGIDA"
        else pd.NA
    ).astype("string")


def _normalize_columns_with_aliases(raw_df: pd.DataFrame, dataset_key: str, aliases: dict[str, Any]) -> pd.DataFrame:
    dataset_aliases = aliases.get(dataset_key, {})
    if dataset_aliases:
        df, _, missing = canonicalize_columns(raw_df, dataset_aliases)
        if missing:
            LOGGER.warning("%s: columnas no localizadas durante canonicalizacion: %s", dataset_key, missing)
        return df
    df = raw_df.copy()
    df.columns = [normalize_column_name(column) for column in df.columns]
    return df


def _add_missing_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df[columns]


def normalize_movimientos(raw_df: pd.DataFrame, aliases: dict[str, Any] | None = None) -> pd.DataFrame:
    aliases = aliases or load_column_aliases()
    df = _normalize_columns_with_aliases(raw_df, "movimientos", aliases)

    alias_candidates = {
        "movement_type": ["tipo_movimiento", "tipo movimiento"],
        "started_at": ["fecha_inicio", "fecha inicio"],
        "completed_at": ["fecha_finalizacion", "fecha finalización", "fecha finalizacion"],
        "sku": ["articulo", "artículo"],
        "sku_description": ["denominacion_articulo", "denominación artículo"],
        "quantity": ["cantidad"],
        "owner": ["propietario"],
        "operator": ["operario", "denominacion_operario"],
        "location": ["ubicacion"],
        "order_id": ["pedido"],
        "external_order_id": ["pedido_externo"],
        "client": ["cliente"],
        "external_client": ["cliente_externo"],
    }
    for target, candidates in alias_candidates.items():
        if target not in df.columns:
            match = next((normalize_column_name(candidate) for candidate in candidates if normalize_column_name(candidate) in df.columns), None)
            if match is not None:
                df[target] = df[match]

    output = pd.DataFrame(index=df.index)
    output["movement_type"] = _text(df.get("movement_type"))
    output["started_at"] = _coerce_datetime(df.get("started_at"))
    output["completed_at"] = _coerce_datetime(df.get("completed_at"))
    output["operational_date"] = output["completed_at"].fillna(output["started_at"]).dt.normalize()
    output["sku"] = _string_code(df.get("sku"))
    output["sku_description"] = _text(df.get("sku_description"))
    output["quantity"] = _coerce_number(df.get("quantity"))
    output["owner"] = _string_code(df.get("owner"))
    output["operator"] = _text(df.get("operator"))
    output["location"] = _string_code(df.get("location"))
    output["order_id"] = _string_code(df.get("order_id"))
    output["external_order_id"] = _string_code(df.get("external_order_id"))
    output["client"] = _string_code(df.get("client"))
    output["external_client"] = _string_code(df.get("external_client"))
    output["generic_code"] = pd.NA
    output["service_code"] = output["external_order_id"]
    output["service_type"] = pd.NA
    output["request_id"] = pd.NA
    output["basket_date"] = output["operational_date"]
    output["transaction_id"] = np.where(
        output["external_order_id"].notna() & output["owner"].notna(),
        output["external_order_id"].astype("string") + "|" + output["owner"].astype("string"),
        pd.NA,
    )
    return _add_missing_columns(output, MOVIMIENTOS_COLUMNS)


def normalize_lineas(raw_df: pd.DataFrame, aliases: dict[str, Any] | None = None) -> pd.DataFrame:
    aliases = aliases or load_column_aliases()
    df = _normalize_columns_with_aliases(raw_df, "solicitudes", aliases)
    output = pd.DataFrame(index=df.index)
    output["request_id"] = _string_code(df.get("id"))
    output["request_code"] = _string_code(df.get("solicitud"))
    output["service_date"] = _coerce_datetime(df.get("fecha_servicio")).dt.normalize()
    output["created_at"] = _coerce_datetime(df.get("creacion_solicitud"))
    output["order_id"] = _string_code(df.get("pedido"))
    output["sku"] = _string_code(df.get("articulo"))
    output["owner"] = _string_code(df.get("propietario"))
    output["department"] = _text(df.get("departamento"))
    output["line_status"] = _text(df.get("estado_linea"))
    output["requested_quantity"] = _coerce_number(df.get("cant_solicitada"))
    output["confirmed_quantity"] = _coerce_number(df.get("cant_confirmada"))
    output["stored_quantity"] = _coerce_number(df.get("cant_almacenada"))
    output["modified_at"] = _coerce_datetime(df.get("modificacion_linea"))
    output["event_end_at"] = _coerce_datetime(df.get("fin_evento"))
    output["deleted_at"] = _coerce_datetime(df.get("borrado_linea"))
    output["line_type"] = _text(df.get("tipo"))
    output["action"] = _text(df.get("accion"))
    output["service_flow"] = _service_flow_from_action(df.get("accion"))
    output["request_status"] = _text(df.get("estado"))
    output["order_created_at"] = _coerce_datetime(df.get("creacion_pedido"))
    output["last_modified_at"] = _coerce_datetime(df.get("ultima_modificacion"))
    output["province"] = _text(df.get("provincia"))
    output["generic_code"] = _string_code(df.get("codigo_generico"))
    return _add_missing_columns(output, LINEAS_COLUMNS)


def normalize_articulos(raw_df: pd.DataFrame, aliases: dict[str, Any] | None = None) -> pd.DataFrame:
    aliases = aliases or load_column_aliases()
    df = _normalize_columns_with_aliases(raw_df, "maestro", aliases)
    output = pd.DataFrame(index=df.index)
    output["sku"] = _string_code(df.get("codigo"))
    output["sku_name"] = _text(df.get("nombre"))
    output["category"] = _text(df.get("categoria"))
    output["created_at"] = _coerce_datetime(df.get("creacion"))
    output["weight_kg"] = _coerce_number(df.get("kilos"))
    output["m2"] = _coerce_number(df.get("m2"))
    output["m3"] = _coerce_number(df.get("m3"))
    output["length"] = _coerce_number(df.get("largo"))
    output["width"] = _coerce_number(df.get("ancho"))
    output["height"] = _coerce_number(df.get("alto"))
    return _add_missing_columns(output.dropna(subset=["sku"]).drop_duplicates("sku"), ARTICULOS_COLUMNS)


def normalize_albaranes(raw_df: pd.DataFrame, aliases: dict[str, Any] | None = None) -> pd.DataFrame:
    aliases = aliases or load_column_aliases()
    df = _normalize_columns_with_aliases(raw_df, "albaranes", aliases)
    output = pd.DataFrame(index=df.index)
    output["item"] = _string_code(df.get("item"))
    output["service_date"] = _coerce_datetime(df.get("fecha_servicio")).dt.normalize()
    output["event_concept"] = _text(df.get("concepto_denominacion_evento_asociado"))
    output["description"] = _text(df.get("descripcion"))
    output["service_code"] = _string_code(df.get("descripcion"))
    output["requester"] = _text(df.get("solicitante_mahou"))
    output["department"] = _text(df.get("dpto_mahou"))
    output["pallets_in"] = _coerce_number(df.get("pales_in"))
    output["boxes_in"] = _coerce_number(df.get("cajas_in"))
    output["m3_in"] = _coerce_number(df.get("m3_in"))
    output["pallets_out"] = _coerce_number(df.get("pales_out"))
    output["boxes_out"] = _coerce_number(df.get("cajas_out"))
    output["m3_out"] = _coerce_number(df.get("m3_out"))
    output["urgency"] = _text(df.get("urgencia"))
    output["holiday"] = _text(df.get("festivo"))
    output["destination_province"] = _text(df.get("provincia_destino"))
    output["weight_kg"] = _coerce_number(df.get("peso_kg_raw"))
    output["volume_m3"] = _coerce_number(df.get("volumen"))
    return _add_missing_columns(output, ALBARANES_COLUMNS)


def _snapshot_date_from_source(path: Path | None) -> pd.Timestamp | pd.NaT:
    if path is None:
        return pd.NaT
    match = re.search(r"(\d{2})-(\d{2})-(\d{4})", path.stem)
    if not match:
        return pd.NaT
    day, month, year = map(int, match.groups())
    return pd.Timestamp(year=year, month=month, day=day).normalize()


def normalize_stock_snapshot(raw_df: pd.DataFrame, source_path: Path | None = None) -> pd.DataFrame:
    df = raw_df.copy()
    df.columns = [normalize_column_name(column) for column in df.columns]
    output = pd.DataFrame(index=df.index)
    output["snapshot_date"] = _snapshot_date_from_source(source_path)
    output["owner"] = _text(df.get("denominacion_propietario"))
    output["owner_code"] = _string_code(df.get("propie"))
    output["sku"] = _string_code(df.get("art_y"))
    output["sku_description"] = _text(df.get("denominacion"))
    output["stock_quantity"] = _coerce_number(df.get("stock_pal"))
    output["location"] = _string_code(df.get("ubicacion"))
    output["occupancy_status"] = _text(df.get("ocupacion"))
    output = output.loc[output["sku"].notna()].copy()
    return _add_missing_columns(output, STOCK_COLUMNS)


def build_normalized_datasets(loaded: LoadedInputs | None = None) -> tuple[dict[str, pd.DataFrame], list[str]]:
    loaded = loaded or load_inputs()
    aliases = load_column_aliases()
    normalized: dict[str, pd.DataFrame] = {}
    warnings = list(loaded.warnings)
    normalizers = {
        "movimientos": normalize_movimientos,
        "lineas": normalize_lineas,
        "articulos": normalize_articulos,
        "albaranes": normalize_albaranes,
    }

    for dataset_name, normalizer in normalizers.items():
        raw = loaded.frames.get(dataset_name)
        if raw is None:
            continue
        try:
            normalized[dataset_name] = normalizer(raw, aliases)
        except Exception as exc:
            warning = f"No se pudo normalizar {dataset_name}: {exc}"
            LOGGER.warning(warning)
            warnings.append(warning)

    if "stock_snapshot" in loaded.frames:
        try:
            normalized["stock_snapshot"] = normalize_stock_snapshot(
                loaded.frames["stock_snapshot"],
                loaded.sources.get("stock_snapshot"),
            )
        except Exception as exc:
            warning = f"No se pudo normalizar stock_snapshot: {exc}"
            LOGGER.warning(warning)
            warnings.append(warning)

    return normalized, warnings


def run_common_pipeline(paths: PipelinePaths | None = None) -> dict[str, Any]:
    paths = paths or get_paths()
    ensure_dirs(paths.input_dir, paths.normalized_dir, paths.output_dir)
    loaded = load_inputs(paths)
    normalized, warnings = build_normalized_datasets(loaded)

    outputs: dict[str, str] = {}
    for dataset_name, frame in normalized.items():
        filename = NORMALIZED_FILES[dataset_name]
        path = paths.normalized_dir / filename
        save_parquet_safe(frame, path, index=False)
        outputs[dataset_name] = str(path)
        LOGGER.info("Dataset normalizado escrito: %s rows=%s", path, len(frame))

    def display_path(path: Path) -> str:
        try:
            return str(path.relative_to(paths.root))
        except ValueError:
            return str(path)

    metadata = {
        "sources": {name: display_path(path) for name, path in loaded.sources.items()},
        "outputs": {name: display_path(Path(path)) for name, path in outputs.items()},
        "warnings": warnings,
    }
    write_json(paths.normalized_dir / "_metadata.json", metadata)
    return metadata
