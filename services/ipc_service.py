import json
import logging
import os

import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, date
from typing import Any, Iterable

from . import config_service


CACHE_PATH = os.path.join("config", "ipc.json")
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


def _month_from_key(key: int) -> str:
    year, month_index = divmod(key, 12)
    return f"{year:04d}-{month_index + 1:02d}"


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


def _normalize_cached_row(
    row: Iterable[Any],
    unofficial_meta: dict[str, Any] | None = None,
) -> list[str] | None:
    if not isinstance(row, (list, tuple)):
        return None
    if not row:
        return None

    mes = parse_fechas(str(row[0]))
    if not mes:
        return None

    source: str | None = None
    if len(row) >= 3:
        raw_source = row[2]
        if isinstance(raw_source, str) and raw_source.strip():
            source = raw_source.strip()

    if source is None and unofficial_meta:
        meta_value = unofficial_meta.get(mes)
        if isinstance(meta_value, dict):
            meta_source = meta_value.get("source")
            if isinstance(meta_source, str) and meta_source.strip():
                source = meta_source.strip()

    if source is None:
        source = "official"

    value_candidate: Any = None
    if len(row) >= 9:
        raw_variacion = row[8]
        if raw_variacion not in (None, ""):
            value_candidate = raw_variacion
    if value_candidate in (None, "") and len(row) >= 2:
        value_candidate = row[1]
    if value_candidate in (None, ""):
        return None

    try:
        value_dec = Decimal(str(value_candidate))
    except (InvalidOperation, ValueError):
        return None

    return [mes, format(value_dec, "f"), source]


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
        fecha = parse_fechas(str(fecha_raw))
        try:
            valor_dec = Decimal(str(valor_raw))
        except (InvalidOperation, TypeError):
            continue
        valor_str = format(valor_dec, "f")
        rows.append([fecha, valor_str, "official"])

    if not rows:
        raise RuntimeError("Respuesta del IPC sin datos")

    rows.sort(key=lambda r: r[0])
    header = ["fecha", "variacion_mensual", "source"]
    return header, rows


def fetch_backup_ipc() -> list[list[str]]:
    """Fetch IPC data from the fallback API and return normalized rows."""

    fallback_url = config_service.get_fallback_api_url()
    response = requests.get(fallback_url, timeout=20)
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("Respuesta del IPC de respaldo inválida") from exc

    if not isinstance(payload, list):
        raise RuntimeError("Respuesta del IPC de respaldo inválida")

    rows: list[list[str]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        fecha_raw = item.get("fecha")
        valor_raw = item.get("valor")
        if not fecha_raw or valor_raw in (None, ""):
            continue
        fecha = parse_fechas(str(fecha_raw))
        try:
            valor_dec = Decimal(str(valor_raw))
        except (InvalidOperation, TypeError):
            continue
        valor_str = format(valor_dec, "f")
        rows.append([fecha, valor_str, "backup"])

    if not rows:
        raise RuntimeError("Respuesta del IPC de respaldo sin datos")

    rows.sort(key=lambda r: r[0])
    return rows


def fetch_ipc_data():
    """Fetch and cache IPC data from the configured API."""
    api_url = config_service.get_api_url()
    fallback_url = config_service.get_fallback_api_url()
    meta = _read_meta()
    cache_exists = os.path.exists(CACHE_PATH)

    raw_unofficial = meta.get("unofficial_months") if isinstance(meta, dict) else {}
    unofficial_months = raw_unofficial if isinstance(raw_unofficial, dict) else {}

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
        "fallback_source": fallback_url,
        "unofficial_months": unofficial_months,
        "contains_unofficial": bool(unofficial_months),
    }

    cached_header: list[str] | None = None
    cached_rows: list[list[str]] | None = None
    cache_is_stale = False
    if cache_exists:
        cached_header, cached_rows = _load_cache_rows()
        latest_month = _latest_cached_month(cached_rows)
        cache_is_stale = _is_cache_stale(latest_month)
        status["stale"] = cache_is_stale
        if unofficial_months:
            cache_is_stale = True
            status["stale"] = True

    if cache_exists and not cache_is_stale and cached_header is not None and cached_rows is not None:
        status["used_cache"] = True
        status["contains_unofficial"] = bool(unofficial_months)
        status["used_backup"] = bool(unofficial_months)
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
            status["contains_unofficial"] = bool(unofficial_months)
            status["used_backup"] = bool(unofficial_months)
            return cached_header, cached_rows, status

        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("Respuesta del IPC inválida") from exc
        header, rows = _parse_api_payload(payload)

        today = datetime.now(timezone.utc)
        today_date = today.date()

        month_rows: dict[str, list[str]] = {}
        month_keys: list[int] = []
        for row in rows:
            if not row:
                continue
            normalized = _normalize_cached_row(row)
            if not normalized:
                continue
            mes = normalized[0]
            key = _month_key(mes)
            if key is not None:
                month_rows[mes] = normalized
                month_keys.append(key)

        cached_map: dict[str, list[str]] = {}
        if cached_rows:
            for cached_row in cached_rows:
                normalized = _normalize_cached_row(cached_row, unofficial_months)
                if not normalized:
                    continue
                cached_map[normalized[0]] = normalized

        missing_months: list[str] = []
        if month_keys:
            start_key = min(month_keys)
            end_key = max(max(month_keys), _required_month_key(today_date))
            for key in range(start_key, end_key + 1):
                month = _month_from_key(key)
                if month not in month_rows:
                    missing_months.append(month)

        unofficial_meta: dict[str, Any] = {}
        backup_rows: list[list[str]] | None = None
        if missing_months:
            try:
                backup_rows = fetch_backup_ipc()
            except Exception as backup_exc:  # noqa: BLE001
                logger.warning("No se pudo obtener el IPC de respaldo: %s", backup_exc)
            backup_map: dict[str, list[str]] = {}
            if backup_rows:
                for backup_row in backup_rows:
                    normalized = _normalize_cached_row(backup_row)
                    if not normalized:
                        continue
                    backup_map[normalized[0]] = normalized

            timestamp = datetime.now(timezone.utc).isoformat()
            for month in missing_months:
                row = backup_map.get(month)
                if row is None:
                    row = cached_map.get(month)
                if row is None:
                    continue
                source_value = row[2] if len(row) > 2 else "backup"
                month_rows[month] = [str(row[0]), str(row[1]), str(source_value)]
                unofficial_meta[month] = {
                    "value": month_rows[month][1],
                    "source": month_rows[month][2] if len(month_rows[month]) > 2 else "backup",
                    "timestamp": timestamp,
                }
            if unofficial_meta:
                status["used_backup"] = True

        for month, row in list(month_rows.items()):
            source = row[2] if len(row) > 2 else "official"
            if source == "official" and month in unofficial_months:
                unofficial_months.pop(month, None)

        unofficial_months.update(unofficial_meta)

        unofficial_months = {
            month: data
            for month, data in unofficial_months.items()
            if month in month_rows and (len(month_rows[month]) < 3 or month_rows[month][2] != "official")
        }

        combined_rows = [
            [str(month_rows[month][0]), str(month_rows[month][1]), month_rows[month][2] if len(month_rows[month]) > 2 else "official"]
            for month in sorted(month_rows.keys(), key=lambda m: (_month_key(m) or 0, m))
        ]

        header = ["fecha", "variacion_mensual", "source"]
        _store_cache(header, combined_rows)
        fetched_at = datetime.now(timezone.utc)
        latest_month = _latest_cached_month(combined_rows)
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
                "contains_unofficial": bool(unofficial_months),
                "unofficial_months": unofficial_months,
            }
        )
        status["used_backup"] = bool(unofficial_months)
        meta_to_store = {
            "etag": status["etag"],
            "last_modified": status["last_modified_header"],
            "fetched_at": fetched_at.isoformat(),
            "unofficial_months": unofficial_months,
            "fallback_source": fallback_url,
        }
        _write_meta(meta_to_store)
        return header, combined_rows, status
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
                    "contains_unofficial": bool(unofficial_months),
                    "unofficial_months": unofficial_months,
                    "used_backup": bool(unofficial_months),
                }
            )
            if cached_header is None or cached_rows is None:
                raise
            return cached_header, cached_rows, status
        try:
            backup_rows = fetch_backup_ipc()
        except Exception:
            raise

        header = ["fecha", "variacion_mensual", "source"]
        combined_map: dict[str, list[str]] = {}
        if cached_rows:
            for row in cached_rows:
                normalized = _normalize_cached_row(row, unofficial_months)
                if not normalized:
                    continue
                combined_map[normalized[0]] = normalized

        backup_map: dict[str, list[str]] = {}
        for row in backup_rows:
            normalized = _normalize_cached_row(row)
            if not normalized:
                continue
            backup_map[normalized[0]] = normalized

        combined_map.update(backup_map)

        ordered_rows = [
            [
                str(combined_map[month][0]),
                str(combined_map[month][1]),
                str(combined_map[month][2]) if len(combined_map[month]) > 2 else "official",
            ]
            for month in sorted(combined_map.keys(), key=lambda m: (_month_key(m) or 0, m))
        ]

        now_dt = datetime.now(timezone.utc)
        timestamp = now_dt.isoformat()
        new_unofficial: dict[str, Any] = {}
        for month, row in combined_map.items():
            source = row[2] if len(row) > 2 else "official"
            if source != "official":
                meta_value = unofficial_months.get(month) if isinstance(unofficial_months, dict) else None
                stored_timestamp = timestamp
                if isinstance(meta_value, dict) and isinstance(meta_value.get("timestamp"), str):
                    stored_timestamp = meta_value["timestamp"]
                if month in backup_map:
                    stored_timestamp = timestamp
                new_unofficial[month] = {
                    "value": row[1],
                    "source": source,
                    "timestamp": stored_timestamp,
                }

        unofficial_months = new_unofficial

        _store_cache(header, ordered_rows)
        meta_to_store = {
            "fetched_at": timestamp,
            "unofficial_months": unofficial_months,
            "fallback_source": fallback_url,
        }
        _write_meta(meta_to_store)

        status.update(
            {
                "used_cache": False,
                "updated": True,
                "error": str(exc),
                "used_backup": True,
                "contains_unofficial": bool(unofficial_months),
                "unofficial_months": unofficial_months,
                "last_cached_at": now_dt,
                "last_checked_at": now_dt,
            }
        )
        return header, ordered_rows, status

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
        if not row:
            continue

        if len(row) >= 9:
            mes = parse_fechas(str(row[0]))
            if not mes:
                continue
            indice_dec: Decimal | None = None
            try:
                indice_dec = Decimal(str(row[1]))
            except (ValueError, InvalidOperation):
                indice_dec = None

            ipc_prop_dec: Decimal | None = None
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
            if ipc_prop_dec is not None:
                out[mes] = ipc_prop_dec
            continue

        normalized = _normalize_cached_row(row)
        if not normalized:
            continue
        mes = normalized[0]
        try:
            value_dec = Decimal(str(normalized[1]))
        except (ValueError, InvalidOperation):
            continue
        if mes:
            out[mes] = value_dec

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
