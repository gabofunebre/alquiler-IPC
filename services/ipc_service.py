import csv
import io
import json
import os
import time
import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime
from .config_service import CSV_URL


CACHE_PATH = os.path.join("config", "ipc.csv")
CACHE_METADATA_PATH = CACHE_PATH + ".meta"
CACHE_TTL = 24 * 60 * 60  # 24 hours in seconds


def _read_cached_rows():
    with open(CACHE_PATH, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if not rows:
        raise RuntimeError("CSV vacío")
    return rows[0], rows[1:]


def _load_metadata():
    try:
        with open(CACHE_METADATA_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return {}


def _store_metadata(headers, preserve_existing=False):
    metadata = {}
    last_modified = headers.get("Last-Modified") if headers else None
    etag = headers.get("ETag") if headers else None
    if last_modified:
        metadata["last_modified"] = last_modified
    if etag:
        metadata["etag"] = etag
    if not metadata:
        if preserve_existing:
            return
        if os.path.exists(CACHE_METADATA_PATH):
            try:
                os.remove(CACHE_METADATA_PATH)
            except OSError:
                pass
        return
    os.makedirs(os.path.dirname(CACHE_METADATA_PATH), exist_ok=True)
    with open(CACHE_METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f)


def leer_csv():
    """Leer y cachear el CSV del IPC."""
    # Usar archivo en caché si existe y es reciente
    cache_exists = os.path.exists(CACHE_PATH)
    if cache_exists:
        age = time.time() - os.path.getmtime(CACHE_PATH)
        if age < CACHE_TTL:
            return _read_cached_rows()

    metadata = _load_metadata() if cache_exists else {}
    headers = {}
    etag = metadata.get("etag")
    last_modified = metadata.get("last_modified")
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    # Descargar CSV y guardar en caché
    try:
        if headers:
            r = requests.get(CSV_URL, timeout=20, headers=headers)
        else:
            r = requests.get(CSV_URL, timeout=20)
    except requests.RequestException:
        if cache_exists:
            return _read_cached_rows()
        raise

    if r.status_code == 304:
        _store_metadata(r.headers, preserve_existing=True)
        if cache_exists:
            try:
                os.utime(CACHE_PATH, None)
            except FileNotFoundError:
                pass
            if os.path.exists(CACHE_METADATA_PATH):
                try:
                    os.utime(CACHE_METADATA_PATH, None)
                except FileNotFoundError:
                    pass
            return _read_cached_rows()
        raise RuntimeError("Respuesta 304 recibida sin caché disponible")

    r.raise_for_status()
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, "wb") as f:
        f.write(r.content)
    rows = list(csv.reader(io.StringIO(r.content.decode("utf-8"))))
    if not rows:
        raise RuntimeError("CSV vacío")
    _store_metadata(r.headers)
    return rows[0], rows[1:]


def parse_fechas(f):
    """Normalize CSV date format to YYYY-MM."""
    try:
        return datetime.strptime(f, "%Y-%m-%d").strftime("%Y-%m")
    except Exception:
        return f[:7]


def ipc_dict():
    """Return a dict {"YYYY-MM": Decimal(proportion)}."""
    _, filas = leer_csv()
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
    return out
