from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.paths import RAW_DIR, ROOT
from src.utils.io_utils import copy_if_needed

LOGGER = logging.getLogger(__name__)


def locate_source_files(settings: dict) -> dict[str, Path]:
    resolved: dict[str, Path] = {}
    for key, filename in settings["files"].items():
        candidates = [ROOT / filename, RAW_DIR / filename]
        match = next((candidate for candidate in candidates if candidate.exists()), None)
        if match is None:
            raise FileNotFoundError(f"Source file not found for {key}: {filename}")
        resolved[key] = match
    return resolved


def ingest_sources(settings: dict) -> dict[str, pd.DataFrame]:
    sources = locate_source_files(settings)
    frames: dict[str, pd.DataFrame] = {}
    for name, source_path in sources.items():
        raw_path = copy_if_needed(source_path, RAW_DIR / source_path.name)
        LOGGER.info("Reading %s from %s", name, raw_path)
        frames[name] = pd.read_excel(raw_path, dtype=object)
    return frames
