import logging
from datetime import date, datetime
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
    current_app,
)

from services.config_service import (
    load_config,
    save_config,
    ADMIN_USER,
    ADMIN_PASS,
    get_csv_url,
)
from services.ipc_service import (
    ipc_dict_with_status,
    get_csv_cache_status,
)
from services.alquiler_service import generar_tabla_alquiler, meses_hasta_fin_anio
from services.user_service import (
    list_users,
    add_user,
    delete_user,
    get_user_config,
    save_user_config,
)

bp = Blueprint("app", __name__)
logger = logging.getLogger(__name__)

def _format_ipc_status(status: dict | None) -> dict | None:
    if not status:
        return None
    formatted = status.copy()
    for key in ("last_cached_at", "last_checked_at"):
        value = formatted.get(key)
        if isinstance(value, datetime):
            formatted[f"{key}_text"] = value.astimezone().strftime("%d-%m-%Y %H:%M %Z")
    return formatted


@bp.get("/health")
def health():
    return {"ok": True}


@bp.get("/ipc/ultimos")
def ipc_ultimos():
    """
    Devuelve los últimos N meses (por defecto 12) con:
      - mes: "YYYY-MM"
      - ipc_mensual: porcentaje mensual en número con 1 decimal (ej: 1.6)
    Fuente: API de series 145.3 (IPC nacional, nivel general, mensual)
    """
    try:
        n = int(request.args.get("n", "12"))
        if n <= 0 or n > 1200:
            abort(400, "Parámetro n inválido")
    except ValueError:
        abort(400, "Parámetro n inválido")

    ipc_data, status = ipc_dict_with_status()

    out = []
    for mes in sorted(ipc_data.keys()):
        ipc_prop_dec = ipc_data[mes]
        ipc_pct_1dec = (
            ipc_prop_dec * Decimal("100")
        ).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
        out.append({"mes": mes, "ipc_mensual": float(ipc_pct_1dec)})

    out = out[-n:]
    last_date = out[-1]["mes"] if out else None
    cache_info = {
        "last_cached_at": status["last_cached_at"].isoformat()
        if status.get("last_cached_at")
        else None,
        "used_cache": status.get("used_cache"),
        "updated": status.get("updated"),
        "stale": status.get("stale"),
        "error": status.get("error"),
    }
    if status.get("last_checked_at"):
        cache_info["last_checked_at"] = status["last_checked_at"].isoformat()
    return jsonify(
        {
            "source": status.get("source") or get_csv_url(),
            "last_month": last_date,
            "count": len(out),
            "data": out,
            "cache": cache_info,
        }
    )


@bp.route("/", methods=["GET", "POST"])
def index():
    if not session.get("user"):
        error = None
        if request.method == "POST":
            nombre = request.form.get("name", "").strip().lower()
            if nombre and nombre in list_users():
                session.clear()
                session["user"] = nombre
                session.permanent = False  # expira al cerrar el navegador
                return redirect(url_for("app.index"))
            error = "Usuario no encontrado"
        return render_template("user_login.html", error=error)

    user_config = get_user_config(session.get("user"))
    tabla = []
    tabla_error = None
    ipc_status = None
    base_raw = user_config.get("alquiler_base")
    inicio_raw = user_config.get("fecha_inicio_contrato", "")
    if base_raw and inicio_raw:
        try:
            base = Decimal(base_raw)
            inicio = inicio_raw[:7]
            periodo = int(user_config.get("periodo_actualizacion_meses") or 3)
            meses = meses_hasta_fin_anio(inicio)
            ipc_data, ipc_status = ipc_dict_with_status()
            tabla = generar_tabla_alquiler(base, inicio, periodo, meses, ipc_data=ipc_data)
        except (InvalidOperation, ValueError) as exc:
            tabla_error = "Configuración inválida. Verificá los datos cargados."
            logger.warning("Configuración inválida para generar tabla de alquiler: %s", exc)
        except Exception as exc:
            logger.exception("Error generando tabla de alquiler")
            tabla_error = "Error cargando IPC. Intentá nuevamente más tarde."

    return render_template(
        "index.html",
        tabla=tabla,
        tabla_error=tabla_error,
        ipc_status=_format_ipc_status(ipc_status),
        fecha_hoy=date.today().strftime("%d-%m-%Y"),
    )


@bp.get("/alquiler/tabla")
def alquiler_tabla():
    user_config = get_user_config(session.get("user"))
    base = request.args.get("alquiler_base")
    if base is None:
        base = user_config.get("alquiler_base")
    inicio = request.args.get("fecha_inicio_contrato")
    if inicio is None:
        inicio = user_config.get("fecha_inicio_contrato", "")
    periodo_value = request.args.get("periodo_actualizacion_meses")
    if periodo_value is None:
        periodo_value = user_config.get("periodo_actualizacion_meses")
    try:
        periodo = int(periodo_value or 3)
    except (TypeError, ValueError):
        abort(400, "periodo_actualizacion_meses inválido")

    if not base or not inicio:
        abort(400, "Faltan parámetros")

    inicio_mes = inicio[:7]
    meses_param = request.args.get("meses")
    if meses_param is not None:
        try:
            meses = int(meses_param)
        except (TypeError, ValueError):
            abort(400, "meses inválido")
    else:
        meses = meses_hasta_fin_anio(inicio_mes)

    try:
        base_dec = Decimal(base)
    except InvalidOperation:
        abort(400, "alquiler_base inválido")

    tabla = generar_tabla_alquiler(base_dec, inicio_mes, periodo, meses)
    return jsonify({"tabla": tabla})


@bp.route("/adm", methods=["GET", "POST"])
def admin():
    """Pantalla de login y configuración"""
    if session.get("logged_in"):
        global_config = load_config()
        csv_url_configured = global_config.get("csv_url", "")
        csv_url_current = get_csv_url()
        csv_cache_info = get_csv_cache_status()
        global_config_extras = {
            key: value
            for key, value in global_config.items()
            if key not in {"csv_url"}
        }

        users = list_users()
        selected_param = request.values.get("selected_user")
        if not selected_param:
            selected_param = request.args.get("user")
        selected_user = selected_param.strip().lower() if selected_param else None
        if selected_user not in users:
            selected_user = users[0] if users else None

        if request.method == "POST":
            form_type = request.form.get("form_type", "user")
            try:
                if form_type == "global":
                    updated_config = global_config.copy()
                    for key, value in request.form.items():
                        if key in {"form_type", "selected_user"}:
                            continue
                        updated_config[key] = value
                    save_config(updated_config)
                    global_config = load_config()
                    csv_url_configured = global_config.get("csv_url", "")
                    csv_url_current = get_csv_url()
                    csv_cache_info = get_csv_cache_status()
                    global_config_extras = {
                        key: value
                        for key, value in global_config.items()
                        if key not in {"csv_url"}
                    }
                else:
                    if not selected_user:
                        abort(400)
                    save_user_config(
                        selected_user,
                        {
                            "alquiler_base": request.form.get("alquiler_base", ""),
                            "fecha_inicio_contrato": request.form.get("fecha_inicio_contrato", ""),
                            "periodo_actualizacion_meses": request.form.get("periodo_actualizacion_meses", ""),
                        },
                    )
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({"ok": True})
            except (TypeError, ValueError) as exc:
                current_app.logger.warning(
                    "Error de validación al guardar la configuración", exc_info=exc
                )
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({"ok": False}), 400
                raise
            except Exception as exc:
                current_app.logger.exception(
                    "Error inesperado al guardar la configuración", exc_info=exc
                )
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({"ok": False}), 500
                raise
            users = list_users()
            if selected_user not in users:
                selected_user = users[0] if users else None

        user_config = get_user_config(selected_user) if selected_user else get_user_config(None)
        tabla = []
        tabla_error = None
        ipc_status = None
        base_raw = user_config.get("alquiler_base")
        inicio_raw = user_config.get("fecha_inicio_contrato", "")
        tiene_config = bool(selected_user and base_raw and inicio_raw)
        if tiene_config:
            try:
                base = Decimal(base_raw)
                inicio = inicio_raw[:7]
                periodo = int(user_config.get("periodo_actualizacion_meses") or 3)
                meses = meses_hasta_fin_anio(inicio)
                ipc_data, ipc_status = ipc_dict_with_status()
                tabla = generar_tabla_alquiler(
                    base, inicio, periodo, meses, ipc_data=ipc_data
                )
            except (InvalidOperation, ValueError) as exc:
                tabla_error = "Configuración inválida. Revisá los datos ingresados."
                logger.warning("Configuración inválida en /adm: %s", exc)
            except Exception as exc:
                logger.exception("Error generando tabla de alquiler en /adm")
                tabla_error = "Error cargando IPC. Intentá nuevamente más tarde."
        return render_template(
            "config.html",
            global_config=global_config,
            global_config_extras=global_config_extras,
            user_config=user_config,
            tabla=tabla,
            tabla_error=tabla_error,
            ipc_status=_format_ipc_status(ipc_status),
            fecha_hoy=date.today().strftime("%d-%m-%Y"),
            users=users,
            contratantes=users,
            selected_user=selected_user,
            tiene_config=tiene_config,
            csv_url_configured=csv_url_configured,
            csv_url_current=csv_url_current,
            csv_cache_info=csv_cache_info,
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
    nombre = request.form.get("new_user", "")
    nuevo = add_user(nombre)
    if nuevo:
        save_user_config(
            nuevo,
            {
                "alquiler_base": request.form.get("alquiler_base", ""),
                "fecha_inicio_contrato": request.form.get(
                    "fecha_inicio_contrato", ""
                ),
                "periodo_actualizacion_meses": request.form.get(
                    "periodo_actualizacion_meses", ""
                ),
            },
        )
        return redirect(url_for("app.admin", user=nuevo))
    return redirect(url_for("app.admin"))


@bp.post("/adm/users/delete")
def admin_delete_user():
    if not session.get("logged_in"):
        abort(403)
    nombre = request.form.get("user", "")
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
