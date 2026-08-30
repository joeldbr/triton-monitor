"""
logging_engine.py
==================

El corazón de la observabilidad de Tritón. Este módulo resuelve dos
responsabilidades acopladas pero distintas:

1. **Formateo JSON forense** (``AsyncJSONFormatter``): traduce cada
   ``LogRecord`` — incluyendo árboles completos de ``ExceptionGroup``,
   causas encadenadas (``__cause__``) y notas dinámicas (``add_note()``)
   — a un string JSON serializado y jerárquico.

2. **Pipeline no bloqueante** (``build_async_logging_pipeline``): desacopla
   físicamente la escritura a disco del bucle de eventos de ``asyncio``
   mediante ``QueueHandler`` + ``QueueListener`` sobre una ``queue.Queue``
   thread-safe, delegando la escritura real a un ``RotatingFileHandler``
   que corre en un hilo secundario y comprime en caliente (Gzip) cada
   archivo rotado.
"""

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
    """
    Formateador de logs que serializa cada ``LogRecord`` como un objeto
    JSON plano y auditable, apto para pipelines de observabilidad
    (ELK, Loki, CloudWatch, etc.).

    Captura de forma estructurada:
    - Timestamp ISO 8601 UTC estricto (``datetime.now(timezone.utc)``).
    - PID (``process``), nombre de hilo (``threadName``) y nombre de tarea
      de asyncio (``taskName``, nativo de Python 3.12+).
    - Cualquier metadato inyectado dinámicamente vía el parámetro ``extra``
      de las llamadas al logger.
    - El árbol recursivo completo de ``ExceptionGroup`` (excepciones
      secundarias, causas raíz encadenadas y notas forenses de
      ``add_note()``), sin truncar ninguna excepción residual del grupo.
    """

    #: Atributos "estándar" de LogRecord que NO deben tratarse como
    #: metadatos dinámicos inyectados vía `extra`.
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

        # Metadatos dinámicos inyectados vía `extra={...}` en las llamadas
        # al logger (ej. logger.error("...", extra={"cluster": "..."})).
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
        """
        Serializa recursivamente una excepción (o un ``ExceptionGroup``)
        en un nodo JSON jerárquico indexable.

        Incluye: tipo, mensaje, traceback completo (vía el módulo
        ``traceback``), notas forenses de ``add_note()``, la causa raíz
        encadenada (``__cause__``) y, si aplica, la lista completa de
        sub-excepciones anidadas de un ``ExceptionGroup``.
        """
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
        # disponible en la causa raíz encadenada.
        cause = exc.__cause__
        if cause is not None:
            node["cause"] = cls._serialize_exception(cause)

        # ExceptionGroup / BaseExceptionGroup: expandir recursivamente
        # cada sub-excepción sin truncar ninguna.
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
    """
    Callback ``rotator``: intercepta el ciclo de rollover, comprime
    atómicamente el archivo histórico recién cerrado a formato Gzip
    (usando la biblioteca nativa ``gzip``) y elimina de forma segura el
    archivo plano residual del sistema operativo.
    """
    with open(source, "rb") as f_in, gzip.open(dest, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(source)


class ForensicQueueHandler(logging.handlers.QueueHandler):
    """
    ``QueueHandler`` especializado que preserva intacto el ``exc_info``
    original de cada ``LogRecord`` al encolarlo.

    Por defecto, ``QueueHandler.prepare()`` pre-formatea el mensaje con el
    formateador de texto estándar de Python (que ya "aplana" cualquier
    traceback a texto plano) y **descarta** ``exc_info`` antes de encolar,
    asumiendo que el registro viajará entre procesos. Como en este pipeline
    usamos una ``queue.Queue`` intra-proceso (no ``multiprocessing.Queue``),
    no hay necesidad de serializar nada: podemos reenviar el ``LogRecord``
    tal cual, conservando el árbol de excepciones completo para que
    ``AsyncJSONFormatter`` lo serialice recursivamente en el hilo del
    ``QueueListener``, sin truncar ninguna excepción residual del grupo.
    """

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return record


class GzipRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """
    ``RotatingFileHandler`` especializado que instala automáticamente los
    callbacks ``namer``/``rotator`` de compresión Gzip en su constructor.

    Se define como clase (en lugar de configurar los callbacks "a mano"
    cada vez) precisamente para poder instanciarlo de forma **declarativa**
    desde ``logging.config.dictConfig`` en ``app_operator.py``, cumpliendo
    con el requisito de configuración declarativa de logging sin perder la
    compresión en caliente de los históricos rotados.
    """

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
    """
    Construye el pipeline de logging asíncrono y no bloqueante:

    - Un ``QueueHandler`` conectado a una ``queue.Queue`` thread-safe se
      adjunta al logger de la aplicación. Encolar un ``LogRecord`` es una
      operación en memoria prácticamente instantánea, por lo que nunca
      bloquea el bucle de eventos de ``asyncio`` mientras se realizan las
      consultas HTTP concurrentes.
    - Un ``QueueListener`` corre en un hilo secundario dedicado, consume
      la cola de forma desatendida y delega la escritura física a un
      ``RotatingFileHandler`` (limitado a 2 MB por archivo, con hasta 3
      backups) que comprime cada rollover a Gzip.

    Returns
    -------
    tuple[logging.Logger, logging.handlers.QueueListener]
        El logger configurado y el ``QueueListener`` YA INICIADO. El
        llamador es responsable de invocar ``listener.stop()`` durante el
        apagado ordenado de la aplicación (ver ``app_operator.py``).
    """
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
