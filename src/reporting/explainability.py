from __future__ import annotations

from pathlib import Path

import pandas as pd


def export_model_explainability(registry: dict, output_path: Path) -> None:
    rows = []
    for key, metadata in registry.items():
        rows.append(metadata | {"registry_key": key})
    pd.DataFrame(rows).to_csv(output_path, index=False)
