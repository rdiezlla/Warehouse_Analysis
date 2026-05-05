from __future__ import annotations

import argparse
import logging

from src.main import main as run_existing_forecast
from src.pipelines.common.normalize import run_common_pipeline

LOGGER = logging.getLogger(__name__)


def run(stage: str = "consumption", normalize_first: bool = True) -> None:
    if normalize_first:
        run_common_pipeline()
    LOGGER.info("Ejecutando Forecast existente con stage=%s", stage)
    run_existing_forecast(stage)


def main() -> None:
    parser = argparse.ArgumentParser(description="Wrapper seguro del pipeline Forecast existente.")
    parser.add_argument(
        "--stage",
        default="consumption",
        choices=["qa", "features", "train", "backtest", "forecast", "consumption", "all"],
        help="Stage del pipeline Forecast existente. Por defecto genera la capa de consumo del dashboard.",
    )
    parser.add_argument("--skip-common", action="store_true", help="No regenerar data/normalized antes de Forecast.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    run(stage=args.stage, normalize_first=not args.skip_common)


if __name__ == "__main__":
    main()
