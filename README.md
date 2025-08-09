# Mini API IPC Argentina (Flask + Docker)

Servicio mínimo en **Flask** que expone, en JSON, la **variación mensual del IPC Nacional (Nivel general)** usando como fuente principal el **CSV oficial** del dataset `145.3` (INDEC / Datos Argentina). Está pensado para calcular y mostrar aumentos de alquiler trimestrales y el detalle **mes a mes**.

---

## Qué hace
- Descarga el CSV “fresco”:  
  `https://infra.datos.gob.ar/catalog/sspm/dataset/145/distribution/145.3/download/indice-precios-al-consumidor-nivel-general-base-diciembre-2016-mensual.csv`
- Lee la **columna 1 (fecha)** y la **columna 2 (índice nacional)**.
- Si el CSV trae la **columna 9** (variación m/m como proporción), la usa.
- Si no, calcula la variación m/m como: `(Índice_t / Índice_{t-1}) - 1`.
- Devuelve **mes (“YYYY-MM”)** e **IPC mensual (%) con 1 decimal**, redondeo **HALF_UP** (ej.: 1,62 → 1,6).

> Motivo: el CSV se actualiza **antes** que la Series API; así obtenés el último mes al momento de la difusión del INDEC.

---

## Endpoints

### `GET /health`
Salud simple.
```json
{ "ok": true }
```

### `GET /ipc/ultimos?n=12`
Devuelve los **últimos N** meses (default 12).

Ejemplo de respuesta:
```json
{
  "source": "https://.../145.3/.../mensual.csv",
  "last_month": "2025-06",
  "count": 12,
  "data": [
    {"mes":"2024-07","ipc_mensual":4.0},
    ...
    {"mes":"2025-06","ipc_mensual":1.6}
  ]
}
```

Parámetros:
- `n` (opcional): cantidad de meses (1–1200).

---

## Ejecución con Docker

### Opción A: Compose (recomendada)
1. **Build & up**  
   ```bash
   make up
   ```
2. El contenedor expone el puerto **8000**. Si mapeaste `6060:8000` en tu `docker-compose.yml`, probá:

   ```bash
   curl -sS http://localhost:6060/health
   curl -sS 'http://localhost:6060/ipc/ultimos?n=12' | jq .
   ```

> Podés ajustar el puerto host en `docker-compose.yml` (`HOST:8000`).

### Opción B: Docker puro
```bash
docker build -t ipc-api .
docker run -d --name ipc-api -p 6060:8000 ipc-api
```

---

## Makefile

Comandos disponibles (usando Docker Compose v2):
```bash
make up           # build + up -d
make down         # down
make rebuild      # down + build --no-cache + up -d --force-recreate
make start        # start (SERVICE=<nombre> opcional)
make stop         # stop  (SERVICE=<nombre> opcional)
make restart      # restart (SERVICE=<nombre> opcional)
```

Variable opcional:
- `SERVICE=ipc-api` para actuar solo sobre ese servicio.

---

## Configuración

Variables de entorno (en `docker-compose.yml` → `environment`):
- `CSV_URL` (opcional): URL del CSV a usar (por defecto, la oficial 145.3).

---

## Notas y consideraciones
- **Redondeo**: 1 decimal, **HALF_UP** (evita el “ban­ker’s rounding”).  
- **Lag de publicación**: el CSV suele reflejar el último mes el **mismo día** de difusión del INDEC (≈ día 13–15). La **Series API** puede demorar; por eso este servicio prioriza CSV.
- **Interpretación**: la respuesta entrega **% mensual** (no índice). Si necesitás índice o acumulados, se puede exponer otra ruta.

---

## Troubleshooting
- **No aparece el último mes**: verificá manualmente el CSV (ej.: `tail -n 3`) y/o que el host tenga salida a Internet.
- **Error de parseo**: a veces hay filas con valores vacíos; el servicio las ignora.
- **Timeout**: ajustá el `timeout` del `requests.get()` según tu red.

---

## Estructura rápida
- `app.py` → Flask app y parsing del CSV (ruta `/ipc/ultimos` y `/health`).
- `Dockerfile` → imagen Python 3.11 + gunicorn.
- `docker-compose.yml` → servicio `ipc-api` (puerto 8000 interno).
- `Makefile` → tareas de orquestación.

---

## Licencia
Uso libre en tu entorno. Si vas a publicar para terceros, agregá la licencia que prefieras (ej. MIT).
