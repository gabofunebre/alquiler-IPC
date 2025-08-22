from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from flask import (
    Blueprint,
    jsonify,
    request,
    abort,
    render_template,
    redirect,
    url_for,
    session,
)

from services.config_service import (
    load_config,
    save_config,
    ADMIN_USER,
    ADMIN_PASS,
    CSV_URL,
)
from services.ipc_service import leer_csv, parse_fechas
from services.alquiler_service import generar_tabla_alquiler, meses_hasta_fin_anio
from services.user_service import load_users, add_user, delete_user

bp = Blueprint("app", __name__)


@bp.get("/health")
def health():
    return {"ok": True}


@bp.get("/ipc/ultimos")
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

    _, filas = leer_csv()

    out = []
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
            ipc_pct_1dec = (
                ipc_prop_dec * Decimal("100")
            ).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
            out.append({"mes": mes, "ipc_mensual": float(ipc_pct_1dec)})
        prev_indice_dec = indice_dec

    out.sort(key=lambda d: d["mes"])
    out = out[-n:]
    last_date = out[-1]["mes"] if out else None
    return jsonify({"source": CSV_URL, "last_month": last_date, "count": len(out), "data": out})


@bp.route("/", methods=["GET", "POST"])
def index():
    if not session.get("user"):
        error = None
        if request.method == "POST":
            nombre = request.form.get("name", "").strip().lower()
            if nombre and nombre in load_users():
                session.clear()
                session["user"] = nombre
                session.permanent = False  # expira al cerrar el navegador
                return redirect(url_for("app.index"))
            error = "Usuario no encontrado"
        return render_template("user_login.html", error=error)

    config = load_config()
    tabla = []
    try:
        base = Decimal(config.get("alquiler_base"))
        inicio = config.get("fecha_inicio_contrato", "")[:7]
        periodo = int(config.get("periodo_actualizacion_meses") or 3)
        meses = meses_hasta_fin_anio(inicio)
        tabla = generar_tabla_alquiler(base, inicio, periodo, meses)
    except Exception:
        tabla = []
    return render_template("index.html", tabla=tabla, fecha_hoy=date.today().strftime("%d-%m-%Y"))


@bp.get("/alquiler/tabla")
def alquiler_tabla():
    config = load_config()
    base = request.args.get("alquiler_base", config.get("alquiler_base"))
    inicio = request.args.get("fecha_inicio_contrato", config.get("fecha_inicio_contrato", "")[:7])
    periodo = int(
        request.args.get(
            "periodo_actualizacion_meses",
            config.get("periodo_actualizacion_meses") or 3,
        )
    )
    meses_param = request.args.get("meses")
    if meses_param is not None:
        meses = int(meses_param)
    else:
        meses = meses_hasta_fin_anio(inicio[:7])
    if not base or not inicio:
        abort(400, "Faltan parámetros")
    try:
        base_dec = Decimal(base)
    except InvalidOperation:
        abort(400, "alquiler_base inválido")
    tabla = generar_tabla_alquiler(base_dec, inicio[:7], periodo, meses)
    return jsonify({"tabla": tabla})


@bp.route("/adm", methods=["GET", "POST"])
def admin():
    """Pantalla de login y configuración"""
    if session.get("logged_in"):
        config = load_config()
        users = load_users()
        if request.method == "POST":
            config.update(
                {
                    "alquiler_base": request.form.get("alquiler_base", ""),
                    "fecha_inicio_contrato": request.form.get("fecha_inicio_contrato", ""),
                    "periodo_actualizacion_meses": request.form.get("periodo_actualizacion_meses", ""),
                }
            )
            try:
                save_config(config)
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
            meses = meses_hasta_fin_anio(inicio)
            tabla = generar_tabla_alquiler(base, inicio, periodo, meses)
        except Exception:
            tabla = []
        return render_template(
            "config.html",
            config=config,
            tabla=tabla,
            fecha_hoy=date.today().strftime("%d-%m-%Y"),
            users=users,
        )

    error = None
    if request.method == "POST":
        if (
            request.form.get("username") == ADMIN_USER
            and request.form.get("password") == ADMIN_PASS
        ):
            session.clear()
            session["logged_in"] = True
            session.permanent = False  # expira al cerrar el navegador
            return redirect(url_for("app.admin"))
        error = "Credenciales inválidas"
    return render_template("login.html", error=error)


@bp.post("/adm/users/add")
def admin_add_user():
    if not session.get("logged_in"):
        abort(403)
    nombre = request.form.get("new_user", "").strip().lower()
    if nombre:
        add_user(nombre)
    return redirect(url_for("app.admin"))


@bp.post("/adm/users/delete")
def admin_delete_user():
    if not session.get("logged_in"):
        abort(403)
    nombre = request.form.get("user", "").strip().lower()
    if nombre:
        delete_user(nombre)
    return redirect(url_for("app.admin"))


@bp.post("/logout")
def logout():
    is_admin = session.get("logged_in")
    session.clear()
    if is_admin:
        return redirect(url_for("app.admin"))
    return redirect(url_for("app.index"))
