from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.paths import DATA_DIR, RAW_DIR, ROOT
from src.utils.io_utils import copy_if_needed

LOGGER = logging.getLogger(__name__)
INPUT_DIR = DATA_DIR / "input"


def _candidate_paths(filename: str) -> list[Path]:
    return [INPUT_DIR / filename, ROOT / filename, RAW_DIR / filename]


def _read_excel_candidate(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, dtype=object)


def locate_source_files(settings: dict) -> dict[str, Path]:
    resolved: dict[str, Path] = {}
    for key, filename in settings["files"].items():
        candidates = _candidate_paths(filename)
        match = next((candidate for candidate in candidates if candidate.exists()), None)
        if match is None:
            raise FileNotFoundError(f"Source file not found for {key}: {filename}")
        resolved[key] = match
    return resolved


def ingest_sources(settings: dict) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for name, filename in settings["files"].items():
        errors: list[str] = []
        for source_path in _candidate_paths(filename):
            if not source_path.exists():
                continue
            try:
                frame = _read_excel_candidate(source_path)
            except Exception as exc:
                errors.append(f"{source_path}: {exc}")
                LOGGER.warning("No se pudo leer %s desde %s: %s", name, source_path, exc)
                continue
            copy_if_needed(source_path, RAW_DIR / source_path.name)
            LOGGER.info("Reading %s from %s", name, source_path)
            frames[name] = frame
            break
        if name not in frames:
            detail = "; ".join(errors) if errors else f"no existe {filename}"
            raise FileNotFoundError(f"Source file not readable for {name}: {detail}")
    return frames
