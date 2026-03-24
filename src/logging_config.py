import logging
from logging.handlers import RotatingFileHandler

from src.paths import LOGS_DIR


def setup_logging(level: str = "INFO") -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger()
    if logger.handlers:
        return
    logger.setLevel(level.upper())
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(LOGS_DIR / "pipeline.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
