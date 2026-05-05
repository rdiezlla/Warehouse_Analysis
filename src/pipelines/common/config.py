from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.paths import CONFIG_DIR, ROOT
from src.utils.io_utils import load_yaml


@dataclass(frozen=True)
class PipelinePaths:
    root: Path = ROOT
    data_dir: Path = ROOT / "data"
    input_dir: Path = ROOT / "data" / "input"
    raw_dir: Path = ROOT / "data" / "raw"
    normalized_dir: Path = ROOT / "data" / "normalized"
    output_dir: Path = ROOT / "data" / "output"
    dashboard_data_dir: Path = ROOT / "dashboard" / "public" / "data"
    legacy_dir: Path = ROOT / "_legacy_imports"


DEFAULT_SOURCE_FILENAMES = {
    "movimientos": "movimientos.xlsx",
    "lineas": "lineas_solicitudes_con_pedidos.xlsx",
    "articulos": "maestro_dimensiones_limpio.xlsx",
    "albaranes": "Informacion_albaranaes.xlsx",
}

CANONICAL_INPUT_FILES = {
    **DEFAULT_SOURCE_FILENAMES,
    "stock_snapshot": "dd-mm-yyyy.xlsx",
}


def get_paths() -> PipelinePaths:
    return PipelinePaths()


def load_column_aliases() -> dict:
    aliases_path = CONFIG_DIR / "column_aliases.yaml"
    if aliases_path.exists():
        return load_yaml(aliases_path)
    return {}
