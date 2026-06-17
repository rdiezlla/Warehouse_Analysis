from __future__ import annotations

import logging
import os
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

EXTERNAL_ORDER_LOOKUP_FILENAMES = (
    "movimientos_pedido_externo_lookup.parquet",
    "movimientos_pedido_externo_lookup.csv",
)
MOVIMIENTOS_LOOKUP_KEY_COLUMNS = ["pallet", "sku", "completed_at", "quantity", "owner"]
MOVIMIENTOS_LOOKUP_FILL_COLUMNS = ["order_id", "external_order_id", "client", "external_client"]


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


def _fill_service_date_from_request(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    service_date = _coerce_datetime(df.get("fecha_servicio")).dt.normalize()
    if "codigo_generico" not in df.columns:
        return service_date, pd.Series(0, index=df.index, dtype="int64")

    request_code = _string_code(df.get("codigo_generico"))
    valid = request_code.notna() & service_date.notna()
    imputed_flag = pd.Series(0, index=df.index, dtype="int64")
    if not valid.any():
        return service_date, imputed_flag

    source = pd.DataFrame({"request_code": request_code.loc[valid], "service_date": service_date.loc[valid]})
    unique_dates = source.drop_duplicates().groupby("request_code")["service_date"].agg(list)
    single_date_by_code = unique_dates[unique_dates.map(len).eq(1)].map(lambda values: values[0])
    fill_values = request_code.map(single_date_by_code)
    fill_mask = service_date.isna() & fill_values.notna()
    service_date = service_date.copy()
    service_date.loc[fill_mask] = fill_values.loc[fill_mask]
    imputed_flag.loc[fill_mask] = 1
    return service_date, imputed_flag


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


def _read_lookup_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Formato de lookup no soportado: {path}")


def _normalize_external_order_lookup(raw_lookup: pd.DataFrame) -> pd.DataFrame:
    lookup = raw_lookup.copy()
    lookup.columns = [normalize_column_name(column) for column in lookup.columns]
    alias_candidates = {
        "pallet": ["paleta"],
        "sku": ["articulo", "artículo"],
        "completed_at": ["fecha_finalizacion", "fecha finalización", "fecha finalizacion"],
        "quantity": ["cantidad"],
        "owner": ["propietario"],
        "order_id": ["pedido"],
        "external_order_id": ["pedido_externo"],
        "client": ["cliente"],
        "external_client": ["cliente_externo"],
    }
    for target, candidates in alias_candidates.items():
        if target not in lookup.columns:
            match = next((normalize_column_name(candidate) for candidate in candidates if normalize_column_name(candidate) in lookup.columns), None)
            if match is not None:
                lookup[target] = lookup[match]

    normalized = pd.DataFrame(index=lookup.index)
    normalized["pallet"] = _string_code(lookup.get("pallet"))
    normalized["sku"] = _string_code(lookup.get("sku"))
    normalized["completed_at"] = _coerce_datetime(lookup.get("completed_at"))
    normalized["quantity"] = _coerce_number(lookup.get("quantity"))
    normalized["owner"] = _string_code(lookup.get("owner"))
    normalized["order_id"] = _string_code(lookup.get("order_id"))
    normalized["external_order_id"] = _string_code(lookup.get("external_order_id"))
    normalized["client"] = _string_code(lookup.get("client"))
    normalized["external_client"] = _string_code(lookup.get("external_client"))

    required_columns = MOVIMIENTOS_LOOKUP_KEY_COLUMNS + ["external_order_id"]
    normalized = normalized.dropna(subset=required_columns)
    normalized = normalized.drop_duplicates(MOVIMIENTOS_LOOKUP_KEY_COLUMNS + MOVIMIENTOS_LOOKUP_FILL_COLUMNS)
    ambiguous = normalized.duplicated(MOVIMIENTOS_LOOKUP_KEY_COLUMNS, keep=False)
    if ambiguous.any():
        LOGGER.warning(
            "Lookup de Pedido externo: se ignoran %s filas con clave ambigua.",
            int(ambiguous.sum()),
        )
        normalized = normalized.loc[~ambiguous].copy()
    return normalized.drop_duplicates(MOVIMIENTOS_LOOKUP_KEY_COLUMNS)


def _load_external_order_lookup(paths: PipelinePaths | None = None) -> pd.DataFrame | None:
    if os.getenv("WAREHOUSE_ENABLE_MOVIMIENTOS_LOOKUP", "").strip().lower() not in {"1", "true", "yes", "si"}:
        return None

    paths = paths or get_paths()
    for filename in EXTERNAL_ORDER_LOOKUP_FILENAMES:
        path = paths.input_dir / filename
        if not path.exists():
            continue
        try:
            lookup = _normalize_external_order_lookup(_read_lookup_table(path))
            LOGGER.info("Lookup de Pedido externo cargado desde %s rows=%s", path, len(lookup))
            return lookup
        except Exception as exc:
            LOGGER.warning("No se pudo cargar el lookup de Pedido externo %s: %s", path, exc)
    return None


def _enrich_external_order_id(output: pd.DataFrame, lookup: pd.DataFrame | None = None) -> pd.DataFrame:
    output = output.copy()
    output["external_order_id_enriched"] = 0
    output["external_order_id_source"] = pd.Series(pd.NA, index=output.index, dtype="string")
    source_mask = output["external_order_id"].notna()
    output.loc[source_mask, "external_order_id_source"] = "source"

    if lookup is None or lookup.empty:
        return output

    missing_mask = (
        output["external_order_id"].isna()
        & output["movement_type"].eq("PI")
        & output[MOVIMIENTOS_LOOKUP_KEY_COLUMNS].notna().all(axis=1)
    )
    if not missing_mask.any():
        return output

    lookup_for_merge = lookup[MOVIMIENTOS_LOOKUP_KEY_COLUMNS + MOVIMIENTOS_LOOKUP_FILL_COLUMNS].rename(
        columns={column: f"{column}_lookup" for column in MOVIMIENTOS_LOOKUP_FILL_COLUMNS}
    )
    candidates = output.loc[missing_mask, MOVIMIENTOS_LOOKUP_KEY_COLUMNS].copy()
    candidates["__row_index"] = candidates.index
    matched = candidates.merge(lookup_for_merge, on=MOVIMIENTOS_LOOKUP_KEY_COLUMNS, how="left")
    matched = matched.loc[matched["external_order_id_lookup"].notna()].copy()
    if matched.empty:
        return output

    matched = matched.drop_duplicates("__row_index")
    row_index = matched["__row_index"]
    for column in MOVIMIENTOS_LOOKUP_FILL_COLUMNS:
        lookup_column = f"{column}_lookup"
        fill_values = pd.Series(matched[lookup_column].to_numpy(), index=row_index)
        fill_mask = output.loc[row_index, column].isna() & fill_values.notna()
        if fill_mask.any():
            output.loc[row_index[fill_mask.to_numpy()], column] = fill_values.loc[fill_mask].to_numpy()

    output.loc[row_index, "external_order_id_enriched"] = 1
    output.loc[row_index, "external_order_id_source"] = "lookup"
    LOGGER.info("Pedido externo enriquecido en movimientos PI: %s filas", len(row_index))
    return output


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
        "pallet": ["paleta"],
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
    output["pallet"] = _string_code(df.get("pallet"))
    output["order_id"] = _string_code(df.get("order_id"))
    output["external_order_id"] = _string_code(df.get("external_order_id"))
    output["client"] = _string_code(df.get("client"))
    output["external_client"] = _string_code(df.get("external_client"))
    output = _enrich_external_order_id(output, _load_external_order_lookup())
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
    service_date, service_date_imputed = _fill_service_date_from_request(df)
    output = pd.DataFrame(index=df.index)
    output["request_id"] = _string_code(df.get("id"))
    output["request_code"] = _string_code(df.get("solicitud"))
    output["service_date"] = service_date
    output["service_date_imputed_from_request"] = service_date_imputed
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
