from __future__ import annotations

from pathlib import Path

from src.features.picking_features import build_picking_datasets
from src.features.service_features import build_service_datasets
from src.utils.io_utils import save_dataframe


def build_and_save_datasets(fact_servicio_dia, fact_picking_dia, dim_date, settings: dict, output_dir: Path) -> dict:
    datasets = {}
    datasets.update(build_service_datasets(fact_servicio_dia, dim_date, settings))
    datasets.update(build_picking_datasets(fact_picking_dia, dim_date, settings))
    for name, df in datasets.items():
        save_dataframe(df, output_dir / name, index=False)
    return datasets
