import os, csv, io, json, requests
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from flask import (
    Flask,
    jsonify,
    request,
    abort,
    render_template,
    redirect,
    url_for,
    session,
)
from dotenv import load_dotenv

CSV_URL = os.getenv(
    "CSV_URL",
    "https://infra.datos.gob.ar/catalog/sspm/dataset/145/distribution/145.3/download/indice-precios-al-consumidor-nivel-general-base-diciembre-2016-mensual.csv",
)

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "changeme")

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config", "config.json")


def _load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {
        "alquiler_base": "",
        "fecha_inicio_contrato": "",
        "periodo_actualizacion_meses": "",
    }


def _save_config(data):
    os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as fh:
        json.dump(data, fh)

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


def _ipc_dict():
    """Devuelve dict {"YYYY-MM": Decimal(proporción)}"""
    _, filas = _leer_csv()
    out = {}
    prev_indice_dec = None
    for row in filas:
        if len(row) < 2:
            continue
        mes = _parse_fechas(row[0])
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


def _add_months(ym: str, m: int) -> str:
    """Suma m meses a 'YYYY-MM'"""
    dt = datetime.strptime(ym, "%Y-%m")
    y = dt.year + ((dt.month - 1 + m) // 12)
    month = (dt.month - 1 + m) % 12 + 1
    return f"{y:04d}-{month:02d}"


def generar_tabla_alquiler(alquiler_base: Decimal, mes_inicio: str, periodo: int, meses: int = 12):
    ipc = _ipc_dict()
    meses_es = [
        "Enero",
        "Febrero",
        "Marzo",
        "Abril",
        "Mayo",
        "Junio",
        "Julio",
        "Agosto",
        "Septiembre",
        "Octubre",
        "Noviembre",
        "Diciembre",
    ]
    tabla = []
    valores = {0: Decimal(alquiler_base).quantize(Decimal("1"), rounding=ROUND_HALF_UP)}
    for i in range(meses):
        ym = _add_months(mes_inicio, i)
        k = i // periodo
        if k not in valores:
            Uk = _add_months(mes_inicio, k * periodo)
            m1 = _add_months(Uk, -3)
            m2 = _add_months(Uk, -2)
            m3 = _add_months(Uk, -1)
            if all(m in ipc for m in (m1, m2, m3)):
                F = (Decimal("1") + ipc[m1]) * (Decimal("1") + ipc[m2]) * (Decimal("1") + ipc[m3])
                valores[k] = (valores[k - 1] * F).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            else:
                valores[k] = valores[k - 1]
        valor = valores[k]
        provisorio = False
        Uk = _add_months(mes_inicio, k * periodo)
        m1 = _add_months(Uk, -3)
        m2 = _add_months(Uk, -2)
        m3 = _add_months(Uk, -1)
        if k > 0 and any(m not in ipc for m in (m1, m2, m3)):
            provisorio = True
        dt = datetime.strptime(ym, "%Y-%m")
        nombre_mes = f"{meses_es[dt.month - 1]} {dt.year}"
        tabla.append({"mes": nombre_mes, "valor": float(valor), "provisorio": provisorio})
    return tabla

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


@app.get("/")
def index():
    config = _load_config()
    tabla = []
    try:
        base = Decimal(config.get("alquiler_base"))
        inicio = config.get("fecha_inicio_contrato", "")[:7]
        periodo = int(config.get("periodo_actualizacion_meses") or 3)
        tabla = generar_tabla_alquiler(base, inicio, periodo)
    except Exception:
        tabla = []
    return render_template("index.html", tabla=tabla)


@app.get("/alquiler/tabla")
def alquiler_tabla():
    config = _load_config()
    base = request.args.get("alquiler_base", config.get("alquiler_base"))
    inicio = request.args.get(
        "fecha_inicio_contrato", config.get("fecha_inicio_contrato", "")[:7]
    )
    periodo = int(
        request.args.get(
            "periodo_actualizacion_meses",
            config.get("periodo_actualizacion_meses") or 3,
        )
    )
    meses = int(request.args.get("meses", "12"))
    if not base or not inicio:
        abort(400, "Faltan parámetros")
    try:
        base_dec = Decimal(base)
    except InvalidOperation:
        abort(400, "alquiler_base inválido")
    tabla = generar_tabla_alquiler(base_dec, inicio[:7], periodo, meses)
    return jsonify({"tabla": tabla})

@app.route("/adm", methods=["GET", "POST"])
def admin():
    """Pantalla de login y configuración"""
    if session.get("logged_in"):
        config = _load_config()
        if request.method == "POST":
            config.update(
                {
                    "alquiler_base": request.form.get("alquiler_base", ""),
                    "fecha_inicio_contrato": request.form.get(
                        "fecha_inicio_contrato", ""
                    ),
                    "periodo_actualizacion_meses": request.form.get(
                        "periodo_actualizacion_meses", ""
                    ),
                }
            )
            try:
                _save_config(config)
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({"ok": True})
            except Exception:
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({"ok": False}), 500
                raise
        tabla = []
        try:
            base = Decimal(config.get("alquiler_base"))
            inicio = config.get("fecha_inicio_contrato", "")[:7]
            periodo = int(config.get("periodo_actualizacion_meses") or 3)
            tabla = generar_tabla_alquiler(base, inicio, periodo)
        except Exception:
            tabla = []
        return render_template("config.html", config=config, tabla=tabla)

    error = None
    if request.method == "POST":
        if (
            request.form.get("username") == ADMIN_USER
            and request.form.get("password") == ADMIN_PASS
        ):
            session.clear()
            session["logged_in"] = True
            session.permanent = False  # expira al cerrar el navegador
            return redirect(url_for("admin"))
        error = "Credenciales inválidas"
    return render_template("login.html", error=error)


@app.post("/logout")
def logout():
    session.clear()
    return redirect(url_for("admin"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
