import json
import logging
import os

import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, date
from typing import Any, Iterable

from . import config_service
from .ipc_errors import translate_ipc_exception


CACHE_PATH = os.path.join("config", "ipc.json")
CACHE_META_PATH = os.path.join("config", "ipc.meta.json")

logger = logging.getLogger(__name__)


def fetch_backup_ipc(
    *, cache_header: list[str] | None = None, cache_rows: list[list[str]] | None = None
) -> tuple[list[str], list[list[str]], dict[str, Any]]:
    """Fetch IPC data from a backup source.

    This function is intended to be patched in tests or implemented by the
    application. The default behavior raises a RuntimeError signalling that the
    backup source is unavailable.
    """

    raise RuntimeError("Fuente de respaldo no disponible")

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


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    total = year * 12 + (month - 1) + delta
    shifted_year, shifted_month_index = divmod(total, 12)
    return shifted_year, shifted_month_index + 1


def _month_key(value: str | None) -> int | None:
    if not value:
        return None
    try:
        year_str, month_str = value.split("-", 1)
        year = int(year_str)
        month = int(month_str)
    except (AttributeError, ValueError):
        return None
    if month < 1 or month > 12:
        return None
    return year * 12 + (month - 1)


def _required_month_key(today: date) -> int:
    delta = -1 if today.day >= 14 else -2
    required_year, required_month = _shift_month(today.year, today.month, delta)
    return required_year * 12 + (required_month - 1)


def _is_cache_stale(latest_month: str | None, *, today: date | None = None) -> bool:
    """Return True when cached data is older than allowed."""

    latest_key = _month_key(latest_month)
    if latest_key is None:
        return True

    if today is None:
        today = datetime.now(timezone.utc).date()
    elif isinstance(today, datetime):
        today = today.date()

    required_key = _required_month_key(today)
    return latest_key < required_key


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


def get_cache_status() -> dict:
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


def _normalize_unofficial_months(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if isinstance(item, str) and item:
            out.append(item)
    return out


def fetch_ipc_data():
    """Fetch and cache IPC data from the configured API."""
    api_url = config_service.get_api_url()
    meta = _read_meta()
    cache_exists = os.path.exists(CACHE_PATH)

    unofficial_months = _normalize_unofficial_months(meta.get("unofficial_months"))

    status = {
        "source": api_url,
        "used_cache": False,
        "updated": False,
        "stale": False,
        "error": None,
        "etag": meta.get("etag"),
        "last_modified_header": meta.get("last_modified"),
        "last_cached_at": _cache_last_modified(),
        "last_checked_at": None,
        "used_backup": False,
        "contains_unofficial": bool(unofficial_months),
        "unofficial_months": unofficial_months,
    }

    cached_header: list[str] | None = None
    cached_rows: list[list[str]] | None = None
    cache_is_stale = False
    if cache_exists:
        cached_header, cached_rows = _load_cache_rows()
        latest_month = _latest_cached_month(cached_rows)
        cache_is_stale = _is_cache_stale(latest_month)
        status["stale"] = cache_is_stale

    if cache_exists and not cache_is_stale and cached_header is not None and cached_rows is not None:
        status["used_cache"] = True
        return cached_header, cached_rows, status

    headers = {}
    if meta.get("etag"):
        headers["If-None-Match"] = meta["etag"]
    if meta.get("last_modified"):
        headers["If-Modified-Since"] = meta["last_modified"]

    try:
        status["last_checked_at"] = datetime.now(timezone.utc)
        response = requests.get(api_url, timeout=20, headers=headers or None)
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
        latest_month = _latest_cached_month(rows)
        cache_is_stale = _is_cache_stale(latest_month)
        status.update(
            {
                "used_cache": False,
                "updated": True,
                "stale": cache_is_stale,
                "last_cached_at": fetched_at,
                "etag": response.headers.get("ETag"),
                "last_modified_header": response.headers.get("Last-Modified"),
                "last_checked_at": fetched_at,
                "used_backup": False,
                "contains_unofficial": False,
                "unofficial_months": [],
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
        error_info = translate_ipc_exception(exc)
        try:
            backup_header, backup_rows, backup_info = fetch_backup_ipc(
                cache_header=cached_header, cache_rows=cached_rows
            )
        except Exception:
            backup_header = backup_rows = None
            backup_info = {}
        else:
            fetched_at = datetime.now(timezone.utc)
            latest_month = _latest_cached_month(backup_rows)
            cache_is_stale = _is_cache_stale(latest_month)
            unofficial_months = _normalize_unofficial_months(
                (backup_info or {}).get("unofficial_months")
            )
            _store_cache(backup_header, backup_rows)
            status.update(
                {
                    "used_cache": False,
                    "updated": True,
                    "stale": cache_is_stale,
                    "last_cached_at": fetched_at,
                    "last_checked_at": fetched_at,
                    "used_backup": True,
                    "contains_unofficial": bool(unofficial_months),
                    "unofficial_months": unofficial_months,
                    "error": error_info.to_dict(),
                    "etag": (backup_info or {}).get("etag"),
                    "last_modified_header": (backup_info or {}).get("last_modified"),
                }
            )
            meta_to_store: dict[str, Any] = {
                "fetched_at": fetched_at.isoformat(),
                "unofficial_months": unofficial_months,
            }
            if status["etag"]:
                meta_to_store["etag"] = status["etag"]
            if status["last_modified_header"]:
                meta_to_store["last_modified"] = status["last_modified_header"]
            _write_meta(meta_to_store)
            return backup_header, backup_rows, status
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
                    "error": error_info.to_dict(),
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
    _, filas, status = fetch_ipc_data()
    out = _rows_to_monthly_variations(filas)
    return out, status


def ipc_dict():
    """Return a dict {"YYYY-MM": Decimal(proportion)} without metadata."""
    data, _ = ipc_dict_with_status()
    return data
