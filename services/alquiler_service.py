from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
from .ipc_service import ipc_dict


MESES_ES = [
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


def add_months(ym: str, m: int) -> str:
    """Add m months to a YYYY-MM string."""
    dt = datetime.strptime(ym, "%Y-%m")
    y = dt.year + ((dt.month - 1 + m) // 12)
    month = (dt.month - 1 + m) % 12 + 1
    return f"{y:04d}-{month:02d}"


def generar_tabla_alquiler(alquiler_base: Decimal, mes_inicio: str, periodo: int, meses: int = 12):
    ipc = ipc_dict()
    hoy_ym = date.today().strftime("%Y-%m")
    max_ym = add_months(hoy_ym, 1)
    tabla = []
    valor_actual = Decimal(alquiler_base).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    valor_periodo = valor_actual
    provisorio_periodo = False

    for i in range(meses):
        ym = add_months(mes_inicio, i)
        if ym > max_ym:
            break
        offset = i % periodo
        ajuste_valor = None
        if offset == 0:
            k = i // periodo
            Uk = add_months(mes_inicio, k * periodo)
            if k > 0:
                meses_previos = [add_months(Uk, -t) for t in range(1, periodo + 1)]
                if all(m in ipc for m in meses_previos):
                    F = Decimal("1")
                    for m in meses_previos:
                        F *= Decimal("1") + ipc[m]
                    nuevo_valor = (valor_actual * F).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
                    ajuste_valor = (nuevo_valor - valor_actual).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
                    valor_periodo = nuevo_valor
                    provisorio_periodo = False
                else:
                    valor_periodo = valor_actual
                    provisorio_periodo = True
            else:
                valor_periodo = valor_actual
                provisorio_periodo = False
            valor_mes = valor_actual
        else:
            valor_mes = valor_periodo
        ipc_pct = None
        if ym in ipc:
            ipc_pct = (ipc[ym] * Decimal("100")).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
        dt = datetime.strptime(ym, "%Y-%m")
        nombre_mes = f"{MESES_ES[dt.month - 1]} {dt.year}"
        future = ym > hoy_ym
        period_idx = i // periodo
        tabla.append(
            {
                "tipo": "mes",
                "mes": nombre_mes,
                "ym": ym,
                "valor": float(valor_mes),
                "provisorio": provisorio_periodo if not future else False,
                "ipc": float(ipc_pct) if ipc_pct is not None else None,
                "future": future,
                "periodo": period_idx,
                "offset": offset,
            }
        )
        if ajuste_valor is not None:
            tabla.append(
                {
                    "tipo": "ajuste",
                    "mes": f"Ajuste {nombre_mes}",
                    "ym": ym,
                    "valor": float(ajuste_valor),
                    "future": future,
                    "periodo": period_idx,
                }
            )
        if offset == periodo - 1 and not provisorio_periodo:
            valor_actual = valor_periodo
    return tabla
