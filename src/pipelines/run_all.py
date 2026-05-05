from __future__ import annotations

import argparse
import logging

from src.pipelines.abc.run_abc_pipeline import run as run_abc
from src.pipelines.common.normalize import run_common_pipeline
from src.pipelines.forecast.run_forecast_pipeline import run as run_forecast
from src.pipelines.market_basket.run_market_basket_pipeline import run as run_market_basket

LOGGER = logging.getLogger(__name__)


def run_all(skip_forecast: bool = False, forecast_stage: str = "consumption") -> None:
    LOGGER.info("Stage 1/4 | Pipeline comun")
    run_common_pipeline()

    if skip_forecast:
        LOGGER.info("Stage 2/4 | Forecast omitido por parametro")
    else:
        LOGGER.info("Stage 2/4 | Forecast")
        run_forecast(stage=forecast_stage, normalize_first=False)

    LOGGER.info("Stage 3/4 | ABC")
    run_abc(normalize_first=False)

    LOGGER.info("Stage 4/4 | Market Basket")
    run_market_basket(normalize_first=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ejecuta todos los pipelines modulares.")
    parser.add_argument("--skip-forecast", action="store_true", help="Ejecuta comun, ABC y Market Basket sin Forecast.")
    parser.add_argument(
        "--forecast-stage",
        default="consumption",
        choices=["qa", "features", "train", "backtest", "forecast", "consumption", "all"],
        help="Stage usado por el wrapper Forecast.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    run_all(skip_forecast=args.skip_forecast, forecast_stage=args.forecast_stage)


if __name__ == "__main__":
    main()
