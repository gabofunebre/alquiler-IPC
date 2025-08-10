import csv
import io
import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime
from .config_service import CSV_URL


def leer_csv():
    """Download and parse the IPC CSV file."""
    r = requests.get(CSV_URL, timeout=20)
    r.raise_for_status()
    rows = list(csv.reader(io.StringIO(r.content.decode("utf-8"))))
    if not rows:
        raise RuntimeError("CSV vacío")
    header, data_rows = rows[0], rows[1:]
    return header, data_rows


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
