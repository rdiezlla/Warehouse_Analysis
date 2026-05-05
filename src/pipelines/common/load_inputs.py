from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.pipelines.common.config import DEFAULT_SOURCE_FILENAMES, PipelinePaths, get_paths

LOGGER = logging.getLogger(__name__)


@dataclass
class LoadedInputs:
    frames: dict[str, pd.DataFrame]
    sources: dict[str, Path]
    warnings: list[str]


def _canonical_input_dirs(paths: PipelinePaths) -> list[Path]:
    return [paths.input_dir]


def _compatibility_input_dirs(paths: PipelinePaths) -> list[Path]:
    return [
        paths.raw_dir,
        paths.root,
        paths.legacy_dir / "abc",
        paths.legacy_dir / "market_basket",
    ]


def _candidate_dirs(paths: PipelinePaths) -> list[Path]:
    return [*_canonical_input_dirs(paths), *_compatibility_input_dirs(paths)]


def _find_named_source(filename: str, paths: PipelinePaths) -> Path | None:
    for directory in _candidate_dirs(paths):
        candidate = directory / filename
        if candidate.exists():
            return candidate
    return None


def _display_path(path: Path, paths: PipelinePaths) -> str:
    try:
        return str(path.relative_to(paths.root))
    except ValueError:
        return str(path)


def _is_canonical_source(path: Path, paths: PipelinePaths) -> bool:
    try:
        path.resolve().relative_to(paths.input_dir.resolve())
        return True
    except ValueError:
        return False


def _compatibility_warning(dataset_name: str, source: Path, paths: PipelinePaths) -> str | None:
    if _is_canonical_source(source, paths):
        return None
    return (
        f"{dataset_name} se cargo desde {_display_path(source, paths)} por compatibilidad. "
        f"Para un proyecto unificado, mueve o copia este input a data/input/."
    )


def _parse_snapshot_date(path: Path) -> pd.Timestamp | None:
    match = re.search(r"(\d{2})-(\d{2})-(\d{4})", path.stem)
    if not match:
        return None
    day, month, year = map(int, match.groups())
    return pd.Timestamp(year=year, month=month, day=day).normalize()


def _find_stock_snapshots_in(directories: list[Path], paths: PipelinePaths) -> list[Path]:
    candidates: list[tuple[pd.Timestamp, int, float, Path]] = []
    excluded = {name.lower() for name in DEFAULT_SOURCE_FILENAMES.values()}
    for directory_priority, directory in enumerate(directories):
        if not directory.exists():
            continue
        for path in directory.glob("*.xlsx"):
            if path.name.lower() in excluded:
                continue
            snapshot_date = _parse_snapshot_date(path)
            if snapshot_date is not None:
                candidates.append((snapshot_date, -directory_priority, path.stat().st_mtime, path))
    if not candidates:
        return []
    candidates.sort(key=lambda item: (item[0], item[1], item[2]))
    return [path for _, _, _, path in reversed(candidates)]


def find_stock_snapshots(paths: PipelinePaths) -> list[Path]:
    canonical = _find_stock_snapshots_in(_canonical_input_dirs(paths), paths)
    if canonical:
        return canonical
    return _find_stock_snapshots_in(_compatibility_input_dirs(paths), paths)


def find_stock_snapshot(paths: PipelinePaths) -> Path | None:
    snapshots = find_stock_snapshots(paths)
    return snapshots[0] if snapshots else None


def _read_tabular(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        return pd.read_excel(path, dtype=object)
    if suffix == ".csv":
        return pd.read_csv(path, dtype=object)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Formato no soportado: {path}")


def load_inputs(paths: PipelinePaths | None = None) -> LoadedInputs:
    paths = paths or get_paths()
    frames: dict[str, pd.DataFrame] = {}
    sources: dict[str, Path] = {}
    warnings: list[str] = []

    for dataset_name, filename in DEFAULT_SOURCE_FILENAMES.items():
        source = _find_named_source(filename, paths)
        if source is None:
            warning = f"No se encontro input para {dataset_name}: {filename}"
            LOGGER.warning(warning)
            warnings.append(warning)
            continue
        try:
            frames[dataset_name] = _read_tabular(source)
            sources[dataset_name] = source
            LOGGER.info("Input %s cargado desde %s", dataset_name, source)
            compatibility_warning = _compatibility_warning(dataset_name, source, paths)
            if compatibility_warning:
                LOGGER.warning(compatibility_warning)
                warnings.append(compatibility_warning)
        except Exception as exc:
            warning = f"No se pudo leer {dataset_name} desde {_display_path(source, paths)}: {exc}"
            LOGGER.warning(warning)
            warnings.append(warning)

    stock_sources = find_stock_snapshots(paths)
    if not stock_sources:
        warning = "No se encontro foto de stock con formato dd-mm-yyyy.xlsx"
        LOGGER.warning(warning)
        warnings.append(warning)
    else:
        for stock_source in stock_sources:
            try:
                frames["stock_snapshot"] = _read_tabular(stock_source)
                sources["stock_snapshot"] = stock_source
                LOGGER.info("Foto de stock cargada desde %s", stock_source)
                compatibility_warning = _compatibility_warning("stock_snapshot", stock_source, paths)
                if compatibility_warning:
                    LOGGER.warning(compatibility_warning)
                    warnings.append(compatibility_warning)
                break
            except Exception as exc:
                warning = f"No se pudo leer foto de stock desde {_display_path(stock_source, paths)}: {exc}"
                LOGGER.warning(warning)
                warnings.append(warning)
        if "stock_snapshot" not in frames:
            warning = "No se pudo leer ninguna foto de stock candidata."
            LOGGER.warning(warning)
            warnings.append(warning)

    return LoadedInputs(frames=frames, sources=sources, warnings=warnings)
