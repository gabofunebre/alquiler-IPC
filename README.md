# Mini API IPC Argentina

Servicio en **Flask** que expone la variación mensual del **IPC nacional** y calcula ajustes de alquiler trimestre a trimestre. Los datos se toman del CSV oficial del INDEC (dataset 145.3).

## Endpoints
- `GET /health`: chequeo simple.
- `GET /ipc/ultimos?n=12`: últimos `n` meses de IPC mensual (por defecto 12).
- `GET /alquiler/tabla`: genera la tabla de alquiler. Requiere `alquiler_base`, `fecha_inicio_contrato` y `periodo_actualizacion_meses`.
- `GET /` y `/adm`: vistas HTML para ver y editar la configuración.

## Ejecución
### Docker Compose
```bash
make up      # build + up -d
default port: 8000
```
Luego probá:
```bash
curl http://localhost:8000/health
```

### Docker
```bash
docker build -t ipc-api .
docker run -d --name ipc-api -p 8000:8000 ipc-api
```

## Configuración
Variables de entorno admitidas:
- `CSV_URL`: URL del CSV a usar (por defecto la oficial del INDEC).
- `ADMIN_USER` y `ADMIN_PASS`: credenciales para `/adm` (por defecto `admin`/`admin`).
- `SECRET_KEY`: clave de sesión de Flask.

## Estructura
- `app.py`: crea la aplicación y registra las rutas.
- `routes.py`: endpoints HTTP.
- `services/`: lógica de IPC, generación de tabla y manejo de configuración.
- `templates/`: archivos HTML.

## Licencia
Uso libre. Agregá la licencia que prefieras si lo publicás.
