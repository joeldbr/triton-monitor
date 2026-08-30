"""
sanitizer.py
============

Validadores declarativos de argumentos CLI (``argparse``). La filosofía de
este módulo es "fail fast en la frontera": ningún dato corrupto, fuera de
rango o mal tipado debe llegar a interactuar con el bucle de eventos de
``asyncio`` ni con los clientes HTTP. Todo se sanea antes de arrancar la
lógica de red.
"""

from __future__ import annotations

import argparse
import re

# Rango permitido (en segundos) para el parámetro --timeout.
_TIMEOUT_MIN = 0.1
_TIMEOUT_MAX = 5.0

# Patrón estricto para identificadores de clúster: cluster-<region>-<numero>
# Ejemplos válidos: cluster-us-east-01, cluster-sa-east-12
_CLUSTER_PATTERN = re.compile(r"^cluster-[a-z]{2}-[a-z]+-\d{2}$")


def validate_timeout(raw_value: str) -> float:
    """
    Validador ``callable`` para el argumento ``--timeout`` de ``argparse``.

    Restringe el parámetro exclusivamente a un rango flotante de 0.1 a 5.0
    segundos. Si el usuario inyecta un dato fuera de rango o un tipo no
    numérico, se lanza ``argparse.ArgumentTypeError`` para que la propia
    CLI termine limpiamente con el código de salida estándar de ``argparse``
    (código de error de sistema 2).

    Parameters
    ----------
    raw_value:
        El string crudo recibido desde la línea de comandos.

    Returns
    -------
    float
        El valor de timeout ya validado y convertido.
    """
    try:
        value = float(raw_value)
    except (TypeError, ValueError) as err:
        raise argparse.ArgumentTypeError(
            f"'{raw_value}' no es un valor numérico válido para --timeout."
        ) from err

    if not (_TIMEOUT_MIN <= value <= _TIMEOUT_MAX):
        raise argparse.ArgumentTypeError(
            f"--timeout debe estar en el rango [{_TIMEOUT_MIN}, {_TIMEOUT_MAX}] "
            f"segundos (recibido: {value})."
        )

    return value


def validate_cluster_id(raw_value: str) -> str:
    """
    Validador ``callable`` para el argumento opcional ``--cluster``.

    Verifica mediante expresiones regulares que el identificador siga de
    forma estricta el patrón ``cluster-<region>-<numero>``
    (ej.: ``cluster-us-east-01``).

    Parameters
    ----------
    raw_value:
        El string crudo recibido desde la línea de comandos.

    Returns
    -------
    str
        El identificador de clúster ya validado.
    """
    if not _CLUSTER_PATTERN.match(raw_value):
        raise argparse.ArgumentTypeError(
            f"'{raw_value}' no cumple el patrón esperado 'cluster-<region>-<numero>' "
            f"(ej.: cluster-us-east-01)."
        )
    return raw_value
