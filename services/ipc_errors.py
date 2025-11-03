from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import requests

ErrorOrigin = Literal["external_service", "internal"]


class PrimarySourceStaleError(RuntimeError):
    """Raised when the primary IPC source does not provide the required month."""

    DEFAULT_MESSAGE = "La API principal no se actualizó"

    def __init__(self, message: str | None = None):
        super().__init__(message or self.DEFAULT_MESSAGE)


@dataclass
class IPCErrorInfo:
    """Structured metadata describing an IPC fetching error."""

    code: str
    origin: ErrorOrigin
    message: str
    detail: str | None = None

    ORIGIN_LABELS = {
        "external_service": "Servicio externo",
        "internal": "Aplicación",
    }

    BADGE_CLASSES = {
        "external_service": "bg-warning text-dark",
        "internal": "bg-secondary",
    }

    def to_dict(self) -> dict:
        data = {
            "code": self.code,
            "origin": self.origin,
            "message": self.message,
        }
        if self.detail:
            data["detail"] = self.detail
        origin_label = self.ORIGIN_LABELS.get(self.origin)
        if origin_label:
            data["origin_label"] = origin_label
        badge_class = self.BADGE_CLASSES.get(self.origin)
        if badge_class:
            data["badge_class"] = badge_class
        return data


def translate_ipc_exception(exc: Exception) -> IPCErrorInfo:
    """Translate a low level exception into an IPCErrorInfo."""

    if isinstance(exc, requests.Timeout):
        return IPCErrorInfo(
            code="timeout",
            origin="external_service",
            message=(
                "El servidor del INDEC tardó demasiado en responder. "
                "Intentá nuevamente más tarde."
            ),
        )

    if isinstance(exc, requests.ConnectionError):
        return IPCErrorInfo(
            code="connection_error",
            origin="external_service",
            message=(
                "No se pudo conectar con el servidor del INDEC para obtener los datos del IPC. "
                "Verificá tu conexión e intentá nuevamente."
            ),
        )

    if isinstance(exc, requests.HTTPError):
        detail = None
        response = exc.response
        if response is not None:
            reason = response.reason or ""
            status_code = response.status_code
            detail = str(status_code)
            if reason:
                detail = f"{detail} {reason}".strip()
        if not detail:
            detail = str(exc) or None
        return IPCErrorInfo(
            code="http_error",
            origin="external_service",
            message=(
                "El servidor del INDEC respondió con un error. "
                "Intentá nuevamente más tarde."
            ),
            detail=detail,
        )

    if isinstance(exc, requests.RequestException):
        return IPCErrorInfo(
            code="request_error",
            origin="external_service",
            message="No se pudo obtener el IPC desde el servidor del INDEC. Intentá nuevamente más tarde.",
            detail=str(exc) or None,
        )

    if isinstance(exc, PrimarySourceStaleError):
        return IPCErrorInfo(
            code="primary_stale",
            origin="external_service",
            message=PrimarySourceStaleError.DEFAULT_MESSAGE,
            detail=str(exc) or None,
        )

    if isinstance(exc, RuntimeError):
        detail = str(exc) or None
        detail_lower = detail.lower() if detail else ""
        cache_related = any(keyword in detail_lower for keyword in ("cache", "almacen"))
        if cache_related:
            return IPCErrorInfo(
                code="cache_error",
                origin="internal",
                message=(
                    "Los datos almacenados del IPC no se pudieron leer. "
                    "Eliminá la cache y volvé a intentarlo."
                ),
                detail=detail,
            )
        return IPCErrorInfo(
            code="invalid_response",
            origin="external_service",
            message=(
                "Los datos recibidos del servidor del INDEC no se pudieron interpretar. "
                "Intentá nuevamente más tarde."
            ),
            detail=detail,
        )

    return IPCErrorInfo(
        code="unexpected_error",
        origin="internal",
        message="Ocurrió un error al obtener los datos del IPC. Intentá nuevamente más tarde.",
        detail=str(exc) or None,
    )
