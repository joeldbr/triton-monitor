
from __future__ import annotations

import gzip
import json
import logging
import logging.handlers
import os
import queue
import shutil
import traceback
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# 1. Formateador JSON forense
# ---------------------------------------------------------------------------
class AsyncJSONFormatter(logging.Formatter):
    
    _RESERVED_ATTRS = frozenset(logging.LogRecord(
        "", 0, "", 0, "", (), None
    ).__dict__.keys()) | {"message", "asctime", "taskName"}

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "process": record.process,
            "threadName": record.threadName,
            "taskName": getattr(record, "taskName", None),
        }

        dynamic_extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in self._RESERVED_ATTRS
        }
        if dynamic_extras:
            payload["extra"] = dynamic_extras

        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            if exc_value is not None:
                payload["exception"] = self._serialize_exception(exc_value)

        return json.dumps(payload, default=str, ensure_ascii=False)

    @classmethod
    def _serialize_exception(cls, exc: BaseException) -> dict[str, Any]:
    
        node: dict[str, Any] = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            ),
        }

        notes = getattr(exc, "__notes__", None)
        if notes:
            node["notes"] = list(notes)

        # Información específica de fallos reales de httpx, cuando esté
        cause = exc.__cause__
        if cause is not None:
            node["cause"] = cls._serialize_exception(cause)

        # ExceptionGroup / BaseExceptionGroup: expandir recursivamente
        sub_exceptions = getattr(exc, "exceptions", None)
        if sub_exceptions:
            node["sub_exceptions"] = [
                cls._serialize_exception(sub_exc) for sub_exc in sub_exceptions
            ]

        return node


# ---------------------------------------------------------------------------
# 2. Compresión en caliente (Gzip) para el RotatingFileHandler
# ---------------------------------------------------------------------------
def _gzip_namer(default_name: str) -> str:
    """Callback ``namer``: renombra el archivo rotado agregando ``.gz``."""
    return f"{default_name}.gz"


def _gzip_rotator(source: str, dest: str) -> None:
   
    with open(source, "rb") as f_in, gzip.open(dest, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(source)


class ForensicQueueHandler(logging.handlers.QueueHandler):

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return record


class GzipRotatingFileHandler(logging.handlers.RotatingFileHandler):
  
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.namer = _gzip_namer
        self.rotator = _gzip_rotator


# ---------------------------------------------------------------------------
# 3. Pipeline no bloqueante: QueueHandler + QueueListener
# ---------------------------------------------------------------------------
def build_async_logging_pipeline(
    logger_name: str = "triton_monitor",
    log_path: str = "production_log.log",
    max_bytes: int = 2 * 1024 * 1024,  # 2 MB
    backup_count: int = 3,
) -> tuple[logging.Logger, logging.handlers.QueueListener]:
    
    log_queue: queue.Queue = queue.Queue()

    queue_handler = ForensicQueueHandler(log_queue)

    file_handler = GzipRotatingFileHandler(
        filename=log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(AsyncJSONFormatter())

    listener = logging.handlers.QueueListener(
        log_queue, file_handler, respect_handler_level=True
    )
    listener.start()

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(queue_handler)
    logger.propagate = False

    return logger, listener
