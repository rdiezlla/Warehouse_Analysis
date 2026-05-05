from __future__ import annotations

import argparse
import logging

import pandas as pd

from src.modeling.abc.analysis import classify_abc
from src.pipelines.common.config import get_paths
from src.pipelines.common.normalize import run_common_pipeline
from src.pipelines.common.schemas import NORMALIZED_FILES
from src.reporting.abc.outputs import write_abc_outputs

LOGGER = logging.getLogger(__name__)


def _read_optional_parquet(path) -> pd.DataFrame:
    if not path.exists():
        LOGGER.warning("No existe %s", path)
        return pd.DataFrame()
    return pd.read_parquet(path)


def run(normalize_first: bool = True) -> dict[str, str]:
    paths = get_paths()
    if normalize_first:
        run_common_pipeline(paths)

    movimientos = _read_optional_parquet(paths.normalized_dir / NORMALIZED_FILES["movimientos"])
    stock = _read_optional_parquet(paths.normalized_dir / NORMALIZED_FILES["stock_snapshot"])
    if stock.empty:
        LOGGER.warning("ABC se ejecuta sin stock_snapshot_normalizado; se calculara ranking solo por movimientos.")

    abc_sku = classify_abc(movimientos, stock if not stock.empty else None)
    outputs = write_abc_outputs(
        abc_sku,
        paths.output_dir / "abc",
        paths.dashboard_data_dir / "abc",
    )
    LOGGER.info("ABC generado con %s SKUs", len(abc_sku))
    return {key: str(value) for key, value in outputs.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline ABC integrado.")
    parser.add_argument("--skip-common", action="store_true", help="No regenerar data/normalized antes de ABC.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    run(normalize_first=not args.skip_common)


if __name__ == "__main__":
    main()
