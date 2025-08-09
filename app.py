import os, csv, io, requests
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from flask import Flask, jsonify, request, abort

CSV_URL = os.getenv(
    "CSV_URL",
    "https://infra.datos.gob.ar/catalog/sspm/dataset/145/distribution/145.3/download/indice-precios-al-consumidor-nivel-general-base-diciembre-2016-mensual.csv",
)

app = Flask(__name__)

def _leer_csv():
    r = requests.get(CSV_URL, timeout=20)
    r.raise_for_status()
    rows = list(csv.reader(io.StringIO(r.content.decode("utf-8"))))
    if not rows:
        raise RuntimeError("CSV vacío")
    header, data_rows = rows[0], rows[1:]
    return header, data_rows

def _parse_fechas(f):
    # "YYYY-MM-01" -> "YYYY-MM"
    try:
        return datetime.strptime(f, "%Y-%m-%d").strftime("%Y-%m")
    except Exception:
        return f[:7]

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/ipc/ultimos")
def ipc_ultimos():
    """
    Devuelve los últimos N meses (por defecto 12) con:
      - mes: "YYYY-MM"
      - ipc_mensual: porcentaje mensual en número con 1 decimal (ej: 1.6)
    Fuente: CSV 145.3 (IPC nacional, nivel general, mensual)
    """
    try:
        n = int(request.args.get("n", "12"))
        if n <= 0 or n > 1200:
            abort(400, "Parámetro n inválido")
    except ValueError:
        abort(400, "Parámetro n inválido")

    _, filas = _leer_csv()

    out = []
    prev_indice_dec = None
    for row in filas:
        if len(row) < 2:
            continue

        mes = _parse_fechas(row[0])

        # Col 2 = índice nacional (base dic-2016=100) como Decimal
        try:
            indice_dec = Decimal(row[1])
        except (ValueError, InvalidOperation):
            continue

        # Col 9: variación m/m como proporción (Decimal), si existe
        ipc_prop_dec = None
        if len(row) >= 9 and row[8] not in ("", None, ""):
            try:
                ipc_prop_dec = Decimal(row[8])
            except (ValueError, InvalidOperation):
                ipc_prop_dec = None

        # Si no viene la col 9, calculo del índice crudo
        if ipc_prop_dec is None and prev_indice_dec is not None and prev_indice_dec > 0:
            ipc_prop_dec = (indice_dec / prev_indice_dec) - Decimal("1")

        if ipc_prop_dec is not None:
            ipc_pct_1dec = (ipc_prop_dec * Decimal("100")).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
            out.append({
                "mes": mes,
                "ipc_mensual": float(ipc_pct_1dec),  # ej: 1.6
            })

        prev_indice_dec = indice_dec

    # Orden cronológico y recorte a últimos N
    out.sort(key=lambda d: d["mes"])
    out = out[-n:]

    last_date = out[-1]["mes"] if out else None
    return jsonify({
        "source": CSV_URL,
        "last_month": last_date,   # ej "2025-06"
        "count": len(out),
        "data": out
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
