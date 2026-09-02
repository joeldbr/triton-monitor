"""
core.py
=======

"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx

from .exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
)

_PROVIDER_ENDPOINTS: dict[str, str] = {
    "AWS": "https://jsonplaceholder.typicode.com/posts/1",
    "Azure": "https://jsonplaceholder.typicode.com/posts/2",
    "GCP": "https://jsonplaceholder.typicode.com/posts/3",
}


@dataclass(slots=True)
class ProviderStatus:
    """Resultado nominal de una consulta exitosa de telemetría."""

    provider: str
    endpoint: str
    status_code: int
    latency_seconds: float


async def _fetch_provider_status(
    client: httpx.AsyncClient,
    provider: str,
    url: str,
) -> ProviderStatus:
    """
    Traduce los fallos nativos de ``httpx`` a la jerarquía semántica de
    excepciones de Tritón, preservando siempre la causa raíz original
    mediante encadenamiento explícito (``raise ... from err``).
    """
    loop = asyncio.get_running_loop()
    start = loop.time()
    try:
        response = await client.get(url)
        response.raise_for_status()
    except httpx.TimeoutException as err:
        elapsed = loop.time() - start
        exc = ProviderTimeoutError(
            f"Timeout al consultar el nodo de telemetría de {provider} ({url})."
        )
        exc.add_note(
            f"Timeout superado en el nodo de telemetría de {provider} "
            f"tras {elapsed:.2f}s de espera real."
        )
        raise exc from err
    except httpx.HTTPStatusError as err:
        exc = CorruptedPayloadError(
            f"Estatus HTTP no esperado recibido de {provider}: "
            f"{err.response.status_code} en {url}."
        )
        exc.add_note(
            f"Verbo={err.request.method} | Status={err.response.status_code} | "
            f"Respuesta cruda={err.response.text[:200]!r}"
        )
        raise exc from err
    except (httpx.ConnectError, httpx.ConnectTimeout) as err:
        exc = NetworkPeeringError(
            f"Colapso de red / resolución DNS al intentar contactar a {provider} ({url})."
        )
        exc.add_note(f"Detalle nativo de httpx: {err!r}")
        raise exc from err

    elapsed = loop.time() - start
    return ProviderStatus(
        provider=provider,
        endpoint=url,
        status_code=response.status_code,
        latency_seconds=elapsed,
    )


async def fetch_timeout_probe(client: httpx.AsyncClient, provider: str = "AWS") -> ProviderStatus:
    """
    Gatillo de timeout real: consulta el endpoint de retardo controlado de
    httpbin, que tarda 3 segundos en responder.
    """
    return await _fetch_provider_status(
        client, provider, "https://httpbin.org/delay/3"
    )


async def fetch_status_probe(
    client: httpx.AsyncClient, provider: str, status_code: int
) -> ProviderStatus:
    """
    Gatillo de estatus HTTP erróneo: consulta el endpoint parametrizable de
    httpbin para forzar una respuesta de error real
    """
    return await _fetch_provider_status(
        client, provider, f"https://httpbin.org/status/{status_code}"
    )


async def scan_all_providers(timeout: float) -> list[ProviderStatus]:
    """
    Orquesta la ejecución paralela y simultánea de las tres consultas de
    telemetría (AWS, Azure, GCP) dentro de un ``asyncio.TaskGroup``.

    """
    results: list[ProviderStatus] = []

    async with httpx.AsyncClient(timeout=timeout) as client:
        async with asyncio.TaskGroup() as task_group:
            tasks = [
                task_group.create_task(
                    _fetch_provider_status(client, provider, url),
                    name=f"telemetry-{provider.lower()}",
                )
                for provider, url in _PROVIDER_ENDPOINTS.items()
            ]

        # Si llegamos aquí, el TaskGroup finalizó sin excepciones agrupadas.
        results = [task.result() for task in tasks]

    return results


async def scan_with_chaos(
    timeout: float,
    inject_timeout: bool = False,
    inject_status_errors: tuple[int, ...] = (),
) -> list[ProviderStatus]:
    """
    Variante de ``scan_all_providers`` usada para pruebas de inyección de
    caos.
    """
    results: list[ProviderStatus] = []

    async with httpx.AsyncClient(timeout=timeout) as client:
        async with asyncio.TaskGroup() as task_group:
            tasks = [
                task_group.create_task(
                    _fetch_provider_status(client, provider, url),
                    name=f"telemetry-{provider.lower()}",
                )
                for provider, url in _PROVIDER_ENDPOINTS.items()
            ]

            if inject_timeout:
                tasks.append(
                    task_group.create_task(
                        fetch_timeout_probe(client, provider="AWS-backup"),
                        name="telemetry-chaos-timeout",
                    )
                )

            for status_code in inject_status_errors:
                tasks.append(
                    task_group.create_task(
                        fetch_status_probe(client, "GCP-backup", status_code),
                        name=f"telemetry-chaos-status-{status_code}",
                    )
                )

        results = [task.result() for task in tasks]

    return results
