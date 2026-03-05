from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Dict

_CST = timezone(timedelta(hours=8))


def setup_json_logger(name: str = "v49") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers = []

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": datetime.now(_CST).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if hasattr(record, "extra_fields") and isinstance(record.extra_fields, dict):
            payload.update(record.extra_fields)
        return json.dumps(payload, ensure_ascii=False)
