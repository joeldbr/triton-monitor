"""
exceptions.py
=============

Jerarquía de excepciones semánticas del sistema de telemetría Tritón.

Todas las excepciones de dominio heredan de ``TritonError``, que a su vez
hereda de la clase estándar ``Exception`` (NUNCA de ``BaseException``).
Heredar de ``BaseException`` es un anti-patrón grave en este contexto:
``BaseException`` es la clase base de señales de control de bajo nivel del
intérprete como ``KeyboardInterrupt`` (Ctrl+C) y ``SystemExit``. Si nuestras
excepciones de dominio heredaran de ella, un bloque ``except*`` (o un
``except`` genérico) mal escrito en otra parte del código podría terminar
"secuestrando" y silenciando esas señales vitales del sistema operativo,
dejando el proceso en un estado no interrumpible. Al heredar de
``Exception`` garantizamos que nuestras excepciones conviven de forma segura
con el manejo estándar de errores de Python.
"""

from __future__ import annotations


class TritonError(Exception):
    """
    Excepción base y semántica para todos los fallos de dominio del
    ecosistema de telemetría Tritón.

    Se hereda explícitamente de ``Exception`` (y no de ``BaseException``)
    para permitir que el programa distinga entre errores de negocio
    recuperables y señales de interrupción del sistema operativo.
    """


class ProviderTimeoutError(TritonError):
    """
    Se lanza cuando una consulta de telemetría hacia un proveedor cloud
    (AWS, Azure o GCP) excede el tiempo máximo de espera configurado.

    Normalmente envuelve (mediante ``raise ... from err``) una
    ``httpx.TimeoutException`` nativa, preservando la causa raíz original
    en ``__cause__`` y agregando contexto forense adicional vía
    ``add_note()``.
    """


class CorruptedPayloadError(TritonError):
    """
    Se lanza cuando un proveedor cloud responde con un status code HTTP
    de error (4xx/5xx) o con un payload corrupto/no procesable.

    Normalmente envuelve una ``httpx.HTTPStatusError`` obtenida mediante
    ``response.raise_for_status()``.
    """


class NetworkPeeringError(TritonError):
    """
    Se lanza ante fallos catastróficos de conectividad de red: colapsos
    de resolución DNS, pérdida de peering entre proveedores o errores de
    conexión de bajo nivel que impiden establecer el socket TCP/TLS.
    """
