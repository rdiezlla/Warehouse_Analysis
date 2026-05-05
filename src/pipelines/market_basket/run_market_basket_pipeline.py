from __future__ import annotations

import argparse
import logging

import pandas as pd

from src.modeling.market_basket.basic import compute_market_basket_outputs
from src.pipelines.common.config import get_paths
from src.pipelines.common.normalize import run_common_pipeline
from src.pipelines.common.schemas import NORMALIZED_FILES
from src.reporting.market_basket.outputs import write_market_basket_outputs

LOGGER = logging.getLogger(__name__)


def run(normalize_first: bool = True) -> dict[str, str]:
    paths = get_paths()
    if normalize_first:
        run_common_pipeline(paths)

    movimientos_path = paths.normalized_dir / NORMALIZED_FILES["movimientos"]
    if not movimientos_path.exists():
        LOGGER.warning("No existe %s; Market Basket se ejecuta sin datos.", movimientos_path)
        movimientos = pd.DataFrame()
    else:
        movimientos = pd.read_parquet(movimientos_path)

    pairs, rules = compute_market_basket_outputs(movimientos)
    outputs = write_market_basket_outputs(
        pairs,
        rules,
        paths.output_dir / "market_basket",
        paths.dashboard_data_dir / "market_basket",
    )
    LOGGER.info("Market Basket generado: pares=%s reglas=%s", len(pairs), len(rules))
    return {key: str(value) for key, value in outputs.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Market Basket integrado.")
    parser.add_argument("--skip-common", action="store_true", help="No regenerar data/normalized antes de Market Basket.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    run(normalize_first=not args.skip_common)


if __name__ == "__main__":
    main()
