import csv
import io
import json
import logging
import os
import time

import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, date

from .config_service import CSV_URL


CACHE_PATH = os.path.join("config", "ipc.csv")
CACHE_METADATA_PATH = CACHE_PATH + ".meta"
CACHE_TTL = 24 * 60 * 60  # 24 hours in seconds
CACHE_META_PATH = os.path.join("config", "ipc.meta.json")

logger = logging.getLogger(__name__)

def _cache_last_modified() -> datetime | None:
    """Return the datetime of the cached CSV if it exists."""

    try:
        ts = os.path.getmtime(CACHE_PATH)
    except OSError:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _load_cache_rows() -> list[list[str]]:
    """Load cached CSV rows."""

    with open(CACHE_PATH, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if not rows:
        raise RuntimeError("CSV vacío")
    return rows


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


def leer_csv():
    """Leer y cachear el CSV del IPC."""
    meta = _read_meta()
    cache_exists = os.path.exists(CACHE_PATH)
    cache_age = None
    if cache_exists:
        cache_age = time.time() - os.path.getmtime(CACHE_PATH)

    status = {
        "source": CSV_URL,
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
        rows = _load_cache_rows()
        cached_header, cached_rows = rows[0], rows[1:]
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
        response = requests.get(CSV_URL, timeout=20, headers=headers or None)
        if response.status_code == 304 and cache_exists:
            if cached_header is None or cached_rows is None:
                rows = _load_cache_rows()
                cached_header, cached_rows = rows[0], rows[1:]
                latest_month = _latest_cached_month(cached_rows)
                cache_is_stale = _is_cache_stale(latest_month)
            os.utime(CACHE_PATH, None)
            status["last_cached_at"] = _cache_last_modified()
            status["used_cache"] = True
            status["stale"] = cache_is_stale
            return cached_header, cached_rows, status

        response.raise_for_status()
        text = response.content.decode("utf-8")
        rows = list(csv.reader(io.StringIO(text)))
        if not rows:
            raise RuntimeError("CSV vacío")
        os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
        with open(CACHE_PATH, "wb") as f:
            f.write(response.content)
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
        return rows[0], rows[1:], status
    except requests.RequestException as exc:
        if cache_exists:
            logger.warning(
                "No se pudo actualizar el CSV del IPC: %s. Se utilizarán los datos cacheados.",
                exc,
            )
            if cached_header is None or cached_rows is None:
                rows = _load_cache_rows()
                cached_header, cached_rows = rows[0], rows[1:]
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
    """Normalize CSV date format to YYYY-MM."""
    try:
        return datetime.strptime(f, "%Y-%m-%d").strftime("%Y-%m")
    except Exception:
        return f[:7]


def ipc_dict_with_status():
    """Return IPC dictionary along with cache status metadata."""
    _, filas, status = leer_csv()
    out = {}
    prev_indice_dec = None
    for row in filas:
        if len(row) < 2:
            continue
        mes = parse_fechas(row[0])
        try:
            indice_dec = Decimal(row[1])
        except (ValueError, InvalidOperation):
            continue
        ipc_prop_dec = None
        if len(row) >= 9 and row[8] not in ("", None, ""):
            try:
                ipc_prop_dec = Decimal(row[8])
            except (ValueError, InvalidOperation):
                ipc_prop_dec = None
        if ipc_prop_dec is None and prev_indice_dec is not None and prev_indice_dec > 0:
            ipc_prop_dec = (indice_dec / prev_indice_dec) - Decimal("1")
        if ipc_prop_dec is not None:
            out[mes] = ipc_prop_dec
        prev_indice_dec = indice_dec
    return out, status


def ipc_dict():
    """Return a dict {"YYYY-MM": Decimal(proportion)} without metadata."""
    data, _ = ipc_dict_with_status()
    return data
