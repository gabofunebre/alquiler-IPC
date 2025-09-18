import json
import logging
import os
import time

import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, date
from typing import Any, Iterable

from . import config_service


CACHE_PATH = os.path.join("config", "ipc.json")
CACHE_TTL = 24 * 60 * 60  # 24 hours in seconds
CACHE_META_PATH = os.path.join("config", "ipc.meta.json")

logger = logging.getLogger(__name__)

def _cache_last_modified() -> datetime | None:
    """Return the datetime of the cached IPC file if it exists."""

    try:
        ts = os.path.getmtime(CACHE_PATH)
    except OSError:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _load_cache_rows() -> tuple[list[str], list[list[str]]]:
    """Load cached IPC rows stored in JSON format."""

    with open(CACHE_PATH, "r", encoding="utf-8") as f:
        try:
            payload = json.load(f)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Cache de IPC inválida") from exc

    header: list[str]
    rows_raw: Iterable[Any]

    if isinstance(payload, dict):
        header_value = payload.get("header")
        header = [str(item) for item in header_value] if isinstance(header_value, list) else []
        rows_raw = payload.get("rows", [])
    elif isinstance(payload, list):
        header = []
        rows_raw = payload
    else:
        raise RuntimeError("Formato de cache desconocido")

    rows: list[list[str]] = []
    for row in rows_raw:
        if isinstance(row, list) and row:
            rows.append([str(col) for col in row])

    if not rows:
        raise RuntimeError("Cache de IPC vacía")

    return header, rows


def _store_cache(header: list[str], rows: list[list[str]]) -> None:
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    payload = {"header": header, "rows": rows}
    with open(CACHE_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)


def _latest_cached_month(rows: list[list[str]]) -> str | None:
    """Return the most recent IPC month found in cached rows."""

    latest: str | None = None
    for row in rows:
        if not row:
            continue
        mes = parse_fechas(str(row[0]))
        if not mes:
            continue
        if latest is None or mes > latest:
            latest = mes
    return latest


def _is_cache_stale(latest_month: str | None, *, today: date | None = None) -> bool:
    """Return True when cached data is older than allowed."""

    if latest_month is None:
        return True

    if today is None:
        today = datetime.now(timezone.utc).date()
    elif isinstance(today, datetime):
        today = today.date()

    months_back = 1 if today.day >= 15 else 2
    total_months = today.year * 12 + (today.month - 1)
    required_total = total_months - months_back
    required_year, required_month_index = divmod(required_total, 12)
    required_month = required_month_index + 1
    required_month_str = f"{required_year:04d}-{required_month:02d}"
    return latest_month < required_month_str


def _read_meta() -> dict:
    try:
        with open(CACHE_META_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError, ValueError):
        return {}


def _write_meta(meta: dict) -> None:
    os.makedirs(os.path.dirname(CACHE_META_PATH), exist_ok=True)
    with open(CACHE_META_PATH, "w", encoding="utf-8") as fh:
        json.dump(meta, fh)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def get_csv_cache_status() -> dict:
    """Return information about the cached IPC data without triggering downloads."""

    has_cache = os.path.exists(CACHE_PATH)
    last_cached_at = _cache_last_modified() if has_cache else None
    meta = _read_meta()
    fetched_at = None
    fetched_at_raw = meta.get("fetched_at") if isinstance(meta, dict) else None
    if isinstance(fetched_at_raw, str):
        fetched_at = _parse_iso_datetime(fetched_at_raw)

    def _fmt(dt: datetime | None) -> str | None:
        if not isinstance(dt, datetime):
            return None
        return dt.astimezone().strftime("%d-%m-%Y %H:%M %Z")

    return {
        "has_cache": bool(has_cache and last_cached_at),
        "last_cached_at": last_cached_at,
        "last_cached_at_text": _fmt(last_cached_at),
        "fetched_at": fetched_at,
        "fetched_at_text": _fmt(fetched_at),
    }


def _parse_api_payload(payload: Any) -> tuple[list[str], list[list[str]]]:
    if not isinstance(payload, dict):
        raise RuntimeError("Respuesta del IPC inválida")
    data = payload.get("data")
    if not isinstance(data, list):
        raise RuntimeError("Respuesta del IPC inválida")

    rows: list[list[str]] = []
    for item in data:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        fecha_raw = item[0]
        valor_raw = item[1]
        if fecha_raw in (None, "") or valor_raw in (None, ""):
            continue
        fecha = str(fecha_raw)
        try:
            valor_dec = Decimal(str(valor_raw))
        except (InvalidOperation, TypeError):
            continue
        valor_str = format(valor_dec, "f")
        rows.append([fecha, valor_str])

    if not rows:
        raise RuntimeError("Respuesta del IPC sin datos")

    rows.sort(key=lambda r: r[0])
    header = ["fecha", "variacion_mensual"]
    return header, rows


def leer_csv():
    """Leer y cachear los datos del IPC desde la API."""
    csv_url = config_service.get_csv_url()
    meta = _read_meta()
    cache_exists = os.path.exists(CACHE_PATH)
    cache_age = None
    if cache_exists:
        cache_age = time.time() - os.path.getmtime(CACHE_PATH)

    status = {
        "source": csv_url,
        "used_cache": False,
        "updated": False,
        "stale": False,
        "error": None,
        "etag": meta.get("etag"),
        "last_modified_header": meta.get("last_modified"),
        "last_cached_at": _cache_last_modified(),
        "last_checked_at": None,
    }

    cached_header: list[str] | None = None
    cached_rows: list[list[str]] | None = None
    cache_is_stale = False
    if cache_exists:
        cached_header, cached_rows = _load_cache_rows()
        latest_month = _latest_cached_month(cached_rows)
        cache_is_stale = _is_cache_stale(latest_month)

    if cache_exists and (cache_age is not None) and cache_age < CACHE_TTL:
        if not cache_is_stale and cached_header is not None and cached_rows is not None:
            status["used_cache"] = True
            return cached_header, cached_rows, status

    headers = {}
    if meta.get("etag"):
        headers["If-None-Match"] = meta["etag"]
    if meta.get("last_modified"):
        headers["If-Modified-Since"] = meta["last_modified"]

    try:
        status["last_checked_at"] = datetime.now(timezone.utc)
        response = requests.get(csv_url, timeout=20, headers=headers or None)
        if response.status_code == 304 and cache_exists:
            if cached_header is None or cached_rows is None:
                cached_header, cached_rows = _load_cache_rows()
                latest_month = _latest_cached_month(cached_rows)
                cache_is_stale = _is_cache_stale(latest_month)
            os.utime(CACHE_PATH, None)
            status["last_cached_at"] = _cache_last_modified()
            status["used_cache"] = True
            status["stale"] = cache_is_stale
            return cached_header, cached_rows, status

        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("Respuesta del IPC inválida") from exc
        header, rows = _parse_api_payload(payload)
        _store_cache(header, rows)
        fetched_at = datetime.now(timezone.utc)
        status.update(
            {
                "used_cache": False,
                "updated": True,
                "stale": False,
                "last_cached_at": fetched_at,
                "etag": response.headers.get("ETag"),
                "last_modified_header": response.headers.get("Last-Modified"),
                "last_checked_at": fetched_at,
            }
        )
        meta_to_store = {
            "etag": status["etag"],
            "last_modified": status["last_modified_header"],
            "fetched_at": fetched_at.isoformat(),
        }
        _write_meta(meta_to_store)
        return header, rows, status
    except (requests.RequestException, RuntimeError) as exc:
        if cache_exists:
            logger.warning(
                "No se pudo actualizar el IPC: %s. Se utilizarán los datos cacheados.",
                exc,
            )
            if cached_header is None or cached_rows is None:
                cached_header, cached_rows = _load_cache_rows()
                latest_month = _latest_cached_month(cached_rows)
                cache_is_stale = _is_cache_stale(latest_month)
            status.update(
                {
                    "used_cache": True,
                    "updated": False,
                    "stale": cache_is_stale,
                    "error": str(exc),
                }
            )
            if cached_header is None or cached_rows is None:
                raise
            return cached_header, cached_rows, status
        raise

def parse_fechas(f):
    """Normalize IPC date format to YYYY-MM."""
    try:
        return datetime.strptime(f, "%Y-%m-%d").strftime("%Y-%m")
    except Exception:
        return f[:7]


def _rows_to_monthly_variations(rows: list[list[str]]) -> dict[str, Decimal]:
    out: dict[str, Decimal] = {}
    prev_index: Decimal | None = None

    for row in rows:
        if len(row) < 2:
            continue
        mes = parse_fechas(str(row[0]))
        indice_dec: Decimal | None = None
        try:
            indice_dec = Decimal(str(row[1]))
        except (ValueError, InvalidOperation):
            indice_dec = None

        ipc_prop_dec: Decimal | None = None
        if len(row) >= 9:
            raw_variacion = row[8]
            if raw_variacion not in ("", None):
                try:
                    ipc_prop_dec = Decimal(str(raw_variacion))
                except (ValueError, InvalidOperation):
                    ipc_prop_dec = None
            if (
                ipc_prop_dec is None
                and indice_dec is not None
                and prev_index is not None
                and prev_index > 0
            ):
                ipc_prop_dec = (indice_dec / prev_index) - Decimal("1")
            if indice_dec is not None:
                prev_index = indice_dec
        else:
            if indice_dec is not None:
                ipc_prop_dec = indice_dec

        if ipc_prop_dec is not None and mes:
            out[mes] = ipc_prop_dec

    return out


def ipc_dict_with_status():
    """Return IPC dictionary along with cache status metadata."""
    _, filas, status = leer_csv()
    out = _rows_to_monthly_variations(filas)
    return out, status


def ipc_dict():
    """Return a dict {"YYYY-MM": Decimal(proportion)} without metadata."""
    data, _ = ipc_dict_with_status()
    return data
