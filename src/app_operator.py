

from __future__ import annotations

import argparse
import asyncio
import logging
import logging.config
import sys

from triton_telemetry import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
    scan_all_providers,
    scan_with_chaos,
    validate_cluster_id,
    validate_timeout,
)

LOG_PATH = "production_log.log"

# ---------------------------------------------------------------------------
# Configuración Declarativa de Logging (dictConfig)
# ---------------------------------------------------------------------------
LOGGING_CONFIG: dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json_forensic": {
            "()": "triton_telemetry.logging_engine.AsyncJSONFormatter",
        },
    },
    "handlers": {
        # Handler físico: escribe en disco, rota a los 2 MB (hasta 3
        # backups) y comprime cada rollover a Gzip.
        "file_handler": {
            "class": "triton_telemetry.logging_engine.GzipRotatingFileHandler",
            "filename": LOG_PATH,
            "maxBytes": 2 * 1024 * 1024,
            "backupCount": 3,
            "encoding": "utf-8",
            "formatter": "json_forensic",
        },
        "queue_handler": {
            "class": "triton_telemetry.logging_engine.ForensicQueueHandler",
            "handlers": ["file_handler"],
            "respect_handler_level": True,
        },
    },
    "loggers": {
        "triton_monitor": {
            "level": "DEBUG",
            "handlers": ["queue_handler"],
            "propagate": False,
        },
    },
}


def build_arg_parser() -> argparse.ArgumentParser:
   
    parser = argparse.ArgumentParser(
        prog="triton-monitor",
        description=(
            "TritonMonitor: CLI oficial de observabilidad multicloud de "
            "Triton Cloud Services (AWS / Azure / GCP)."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=validate_timeout,
        default=2.0,
        metavar="[0.1-5.0]",
        help="Timeout (segundos) para las consultas HTTP asíncronas.",
    )
    parser.add_argument(
        "--cluster",
        type=validate_cluster_id,
        default=None,
        metavar="cluster-<region>-<numero>",
        help="Identificador opcional de clúster (ej.: cluster-us-east-01).",
    )
    parser.add_argument(
        "--mode",
        choices=["nominal", "debug", "emergency"],
        default="nominal",
        help=(
            "Modo operativo: 'nominal' (chequeo estándar), 'debug' "
            "(inyecta fallos de caos controlados) o 'emergency' "
            "(fuerza timeouts agresivos de baja latencia)."
        ),
    )

    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--quiet",
        action="store_true",
        help="Suprime la salida de texto en consola (solo deja logs en disco).",
    )
    output_group.add_argument(
        "--verbose",
        action="store_true",
        help="Amplía la salida de texto en consola con detalle forense.",
    )

    return parser


async def _run_scan(args: argparse.Namespace) -> list:
    """Ejecuta el escaneo de telemetría según el modo operativo elegido."""
    if args.mode == "nominal":
        return await scan_all_providers(timeout=args.timeout)

    if args.mode == "debug":
        return await scan_with_chaos(
            timeout=args.timeout,
            inject_timeout=True,
            inject_status_errors=(504, 422),
        )

    return await scan_with_chaos(
        timeout=min(args.timeout, 0.3),
        inject_timeout=True,
        inject_status_errors=(504,),
    )


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger("triton_monitor")

    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None and getattr(queue_handler, "listener", None):
        queue_handler.listener.start()

    if not args.quiet:
        print(
            f"[TritonMonitor] Iniciando escaneo en modo '{args.mode}' "
            f"(timeout={args.timeout}s, cluster={args.cluster or 'N/A'})..."
        )

    try:
        #    tipo de fallo de dominio, sin mezclar `except` normal aquí.
        try:
            results = asyncio.run(_run_scan(args))
        except* ProviderTimeoutError as timeout_group:
            logger.error(
                "Fallos de timeout detectados durante el escaneo de telemetría.",
                exc_info=(type(timeout_group), timeout_group, timeout_group.__traceback__),
            )
            if not args.quiet:
                print(
                    f"[ALERTA] {len(timeout_group.exceptions)} nodo(s) excedieron "
                    "el tiempo de espera configurado:"
                )
                for sub_exc in timeout_group.exceptions:
                    print(f"  - {sub_exc}")
                    for note in getattr(sub_exc, "__notes__", []):
                        print(f"    nota forense: {note}")
        except* CorruptedPayloadError as payload_group:
            logger.warning(
                "Respuestas HTTP corruptas o con status de error mitigadas.",
                exc_info=(type(payload_group), payload_group, payload_group.__traceback__),
            )
            if not args.quiet:
                print(
                    f"[MITIGADO] {len(payload_group.exceptions)} respuesta(s) HTTP "
                    "con estatus no esperado (el sistema continúa operando):"
                )
                for sub_exc in payload_group.exceptions:
                    print(f"  - {sub_exc}")
        except* NetworkPeeringError as peering_group:
            logger.critical(
                "Fallos catastróficos de peering/DNS detectados.",
                exc_info=(type(peering_group), peering_group, peering_group.__traceback__),
            )
            if not args.quiet:
                print(
                    f"[CRÍTICO] {len(peering_group.exceptions)} nodo(s) sufrieron "
                    "colapso de red/DNS:"
                )
                for sub_exc in peering_group.exceptions:
                    print(f"  - {sub_exc}")
        else:
            logger.info(
                "Escaneo de telemetría completado sin fallos.",
                extra={"cluster": args.cluster, "mode": args.mode},
            )
            if not args.quiet:
                print(f"[OK] {len(results)} nodo(s) reportaron estado nominal:")
                for status in results:
                    print(
                        f"  - {status.provider}: HTTP {status.status_code} "
                        f"({status.latency_seconds:.3f}s)"
                    )
    finally:
        #    prohibido usar return/break/continue dentro de este bloque
        if queue_handler is not None and getattr(queue_handler, "listener", None):
            queue_handler.listener.stop()
        if not args.quiet:
            print("[TritonMonitor] Pipeline de logging cerrado de forma ordenada.")


if __name__ == "__main__":
    main()
