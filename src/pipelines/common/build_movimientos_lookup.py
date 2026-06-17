from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from src.pipelines.common.config import get_paths
from src.pipelines.common.normalize import _normalize_external_order_lookup
from src.utils.io_utils import save_parquet_safe
from src.utils.text_utils import normalize_column_name, normalize_text

LOGGER = logging.getLogger(__name__)


def _read_source(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Formato no soportado para lookup de movimientos: {path}")


def build_lookup(source_path: Path, output_base: Path | None = None) -> dict[str, Path | int]:
    paths = get_paths()
    output_base = output_base or paths.input_dir / "movimientos_pedido_externo_lookup"
    raw = _read_source(source_path)
    raw.columns = [normalize_column_name(column) for column in raw.columns]

    if "tipo_movimiento" in raw.columns:
        movement_type = raw["tipo_movimiento"].map(lambda value: normalize_text(value) if pd.notna(value) else "")
        raw = raw.loc[movement_type.eq("PI")].copy()

    lookup = _normalize_external_order_lookup(raw)
    output_base.parent.mkdir(parents=True, exist_ok=True)
    csv_path = output_base.with_suffix(".csv")
    parquet_path = output_base.with_suffix(".parquet")
    lookup.to_csv(csv_path, index=False)
    save_parquet_safe(lookup, parquet_path, index=False)
    LOGGER.info("Lookup escrito: %s rows=%s", parquet_path, len(lookup))
    return {"rows": len(lookup), "csv": csv_path, "parquet": parquet_path}


def main() -> None:
    paths = get_paths()
    parser = argparse.ArgumentParser(
        description="Construye el lookup local para recuperar Pedido externo historico de movimientos PI."
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=paths.legacy_dir / "market_basket" / "movimientos.xlsx",
        help="Archivo historico con Paleta, Articulo, Fecha finalizacion y Pedido externo.",
    )
    parser.add_argument(
        "--output-base",
        type=Path,
        default=paths.input_dir / "movimientos_pedido_externo_lookup",
        help="Ruta base sin extension para escribir CSV y Parquet.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    result = build_lookup(args.source, args.output_base)
    LOGGER.info("Lookup generado: %s", result)


if __name__ == "__main__":
    main()
