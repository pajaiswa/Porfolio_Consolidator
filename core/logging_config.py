"""
logging_config.py — Centralised Logging Setup
===============================================
Call `configure_logging()` once in run_all.py (or any entry-point script).
Individual pipeline modules never call basicConfig — they just do:

    import logging
    logger = logging.getLogger(__name__)

The LOG_LEVEL environment variable controls verbosity at runtime:

    LOG_LEVEL=DEBUG uv run python run_all.py   # full detail
    LOG_LEVEL=WARNING uv run python run_all.py # only warnings / errors
    uv run python run_all.py                   # default: INFO
"""
import logging
import os
from pathlib import Path


LOG_FMT  = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s"
DATE_FMT = "%H:%M:%S"


def configure_logging(log_file: str | None = "data/output/pipeline.log") -> None:
    """
    Sets up the root logger with console + optional file output.

    Args:
        log_file: Path to write a persistent log file.  Pass None to disable.
    """
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handlers: list[logging.Handler] = [logging.StreamHandler()]

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format=LOG_FMT,
        datefmt=DATE_FMT,
        handlers=handlers,
        force=True,   # override any prior basicConfig calls (e.g. from libraries)
    )

    # Silence noisy third-party loggers that pollute the output
    for noisy in ("urllib3", "requests", "mftool", "pdfplumber", "PIL"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logging.getLogger(__name__).debug("Logging configured (level=%s)", level_name)
