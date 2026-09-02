

from __future__ import annotations

import argparse
import re

_TIMEOUT_MIN = 0.1
_TIMEOUT_MAX = 5.0

_CLUSTER_PATTERN = re.compile(r"^cluster-[a-z]{2}-[a-z]+-\d{2}$")


def validate_timeout(raw_value: str) -> float:
    
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
   
    if not _CLUSTER_PATTERN.match(raw_value):
        raise argparse.ArgumentTypeError(
            f"'{raw_value}' no cumple el patrón esperado 'cluster-<region>-<numero>' "
            f"(ej.: cluster-us-east-01)."
        )
    return raw_value
