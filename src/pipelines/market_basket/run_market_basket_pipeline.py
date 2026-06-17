from __future__ import annotations

import argparse
import logging

import pandas as pd

from src.modeling.market_basket.basic import compute_market_basket_outputs
from src.modeling.market_basket.config import AppConfig, ColumnConfig, PathConfig, validate_config
from src.modeling.market_basket.pipeline import run_pipeline
from src.pipelines.common.config import get_paths
from src.pipelines.common.normalize import run_common_pipeline
from src.pipelines.common.schemas import NORMALIZED_FILES
from src.reporting.market_basket.outputs import write_market_basket_outputs
from src.utils.io_utils import ensure_dirs

LOGGER = logging.getLogger(__name__)


def _write_dashboard_json(frame: pd.DataFrame, path, limit: int = 5000) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.head(limit).to_json(path, orient="records", force_ascii=False, indent=2, date_format="iso")


def run(normalize_first: bool = True, basic: bool = False) -> dict[str, str]:
    paths = get_paths()
    if normalize_first:
        run_common_pipeline(paths)

    movimientos_path = paths.normalized_dir / NORMALIZED_FILES["movimientos"]
    if not movimientos_path.exists():
        LOGGER.warning("No existe %s; Market Basket se ejecuta sin datos.", movimientos_path)
        movimientos = pd.DataFrame()
    else:
        movimientos = pd.read_parquet(movimientos_path)

    if not basic and movimientos_path.exists():
        output_dir = paths.output_dir / "market_basket"
        dashboard_dir = paths.dashboard_data_dir / "market_basket"
        ensure_dirs(output_dir, output_dir / "logs", output_dir / "plots", dashboard_dir)
        config = validate_config(
            AppConfig(
                paths=PathConfig(
                    input_data=str(movimientos_path),
                    output_dir=str(output_dir),
                    logs_dir=str(output_dir / "logs"),
                    plots_dir=str(output_dir / "plots"),
                ),
                columns=ColumnConfig(
                    movement_type="movement_type",
                    completion_date="operational_date",
                    article="sku",
                    article_description="sku_description",
                    quantity="quantity",
                    owner="owner",
                    location="location",
                    external_order="external_order_id",
                ),
            )
        )
        artifacts = run_pipeline(config)
        alias_outputs = write_market_basket_outputs(
            artifacts.scoring.scored_pairs,
            artifacts.associations.rule_metrics,
            output_dir,
            dashboard_dir,
        )
        dashboard_frames = {
            "afinidad_pares": artifacts.scoring.scored_pairs,
            "afinidad_reglas": artifacts.associations.rule_metrics,
            "kpi_resumen": artifacts.eda.kpi_summary,
            "calidad_datos": artifacts.cleaning.quality_summary,
            "transacciones_resumen": artifacts.eda.transaction_summary,
            "articulos_resumen": artifacts.eda.article_summary,
            "item_metrics": artifacts.associations.item_metrics,
            "clusters_sku": artifacts.clusters.cluster_summary,
            "hubs_sku": artifacts.clusters.hub_summary,
            "temporal_stability_metrics": artifacts.temporal.stability_metrics,
        }
        for name, frame in dashboard_frames.items():
            _write_dashboard_json(frame, dashboard_dir / f"{name}.json")
        LOGGER.info(
            "Market Basket completo generado: pares=%s reglas=%s transacciones=%s",
            len(artifacts.scoring.scored_pairs),
            len(artifacts.associations.rule_metrics),
            len(artifacts.transactions.transactions_df),
        )
        return {key: str(value) for key, value in alias_outputs.items()}

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
    parser.add_argument("--basic", action="store_true", help="Ejecuta solo el calculo basico de pares/reglas.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    run(normalize_first=not args.skip_common, basic=args.basic)


if __name__ == "__main__":
    main()
