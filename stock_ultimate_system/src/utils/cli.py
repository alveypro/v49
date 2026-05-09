from __future__ import annotations

import logging
import sys
from pathlib import Path

from src.utils.project_paths import resolve_project_path


LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s - %(message)s'


def setup_cli_logging(log_file: str | Path | None = None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file is not None:
        path = resolve_project_path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(path, encoding='utf-8'))
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=handlers)
