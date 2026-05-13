import logging
from collections.abc import Iterable

LOGGER = logging.getLogger(__name__)


def require_columns(dataset_name: str, missing: Iterable[str], *, critical: bool = False) -> None:
    missing = [col for col in missing if col]
    if not missing:
        return
    message = f"{dataset_name}: missing columns -> {missing}"
    if critical:
        raise KeyError(message)
    LOGGER.warning(message)


def warn_if_threshold(name: str, value: float, threshold: float, comparison: str = ">") -> None:
    trigger = value > threshold if comparison == ">" else value < threshold
    if trigger:
        LOGGER.warning("Threshold triggered for %s: value=%.4f threshold=%s%.4f", name, value, comparison, threshold)
