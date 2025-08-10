import csv
import io
import os
import time
import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime
from .config_service import CSV_URL


CACHE_PATH = os.path.join("config", "ipc.csv")
CACHE_TTL = 24 * 60 * 60  # 24 hours in seconds


def leer_csv():
    """Leer y cachear el CSV del IPC."""
    # Usar archivo en caché si existe y es reciente
    if os.path.exists(CACHE_PATH):
        age = time.time() - os.path.getmtime(CACHE_PATH)
        if age < CACHE_TTL:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                rows = list(csv.reader(f))
            if not rows:
                raise RuntimeError("CSV vacío")
            return rows[0], rows[1:]

    # Descargar CSV y guardar en caché
    r = requests.get(CSV_URL, timeout=20)
    r.raise_for_status()
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, "wb") as f:
        f.write(r.content)
    rows = list(csv.reader(io.StringIO(r.content.decode("utf-8"))))
    if not rows:
        raise RuntimeError("CSV vacío")
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
