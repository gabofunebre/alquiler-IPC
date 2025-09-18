import json
import os
from typing import Any, Dict

from dotenv import load_dotenv

load_dotenv()

DEFAULT_API_URL = os.getenv(
    "IPC_API_URL",
    "https://apis.datos.gob.ar/series/api/series?ids=145.3_INGNACUAL_DICI_M_38&format=json&start_date=2016-01&limit=1000",
)
DEFAULT_GLOBAL_CONFIG = {"api_url": DEFAULT_API_URL}
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")

USER_CONFIG_KEYS = {
    "nombre",
    "apellido",
    "dni",
    "direccion",
    "telefono",
    "mail",
    "fecha_inicio_contrato",
    "valor_inicial_contrato",
    "periodo_actualizacion_meses",
    "inmueble_locado",
    "alquiler_base",
}


def _sanitize_global_config(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}

    sanitized = {k: v for k, v in data.items() if k not in USER_CONFIG_KEYS}

    api_url_value = sanitized.get("api_url")
    if isinstance(api_url_value, str):
        stripped = api_url_value.strip()
        if stripped:
            sanitized["api_url"] = stripped
        else:
            sanitized.pop("api_url", None)
    elif "api_url" in sanitized:
        sanitized.pop("api_url")

    return sanitized


def _write_config(data: Dict[str, Any]) -> None:
    path = os.path.abspath(CONFIG_FILE)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    to_store: Any
    if isinstance(data, dict):
        to_store = DEFAULT_GLOBAL_CONFIG.copy()
        to_store.update(data)
        api_url_value = to_store.get("api_url")
        if isinstance(api_url_value, str):
            to_store["api_url"] = api_url_value.strip()
    else:
        to_store = data
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(to_store, fh, indent=2, sort_keys=True)


def load_config() -> Dict[str, Any]:
    """Load global configuration from JSON file."""

    path = os.path.abspath(CONFIG_FILE)
    if not os.path.exists(path):
        _write_config(DEFAULT_GLOBAL_CONFIG)
        return DEFAULT_GLOBAL_CONFIG.copy()
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except (json.JSONDecodeError, OSError):
        _write_config(DEFAULT_GLOBAL_CONFIG)
        return DEFAULT_GLOBAL_CONFIG.copy()
    sanitized = _sanitize_global_config(raw)
    merged = DEFAULT_GLOBAL_CONFIG.copy()
    if isinstance(sanitized, dict):
        merged.update(sanitized)
    if merged != raw:
        _write_config(merged)
    return merged


def save_config(data: Dict[str, Any]) -> None:
    """Persist global configuration to JSON file."""

    sanitized = _sanitize_global_config(data)
    merged = DEFAULT_GLOBAL_CONFIG.copy()
    if isinstance(sanitized, dict):
        merged.update(sanitized)
    _write_config(merged)


def get_api_url() -> str:
    """Return the configured IPC API URL, falling back to defaults."""

    config = load_config()
    api_url = config.get("api_url")
    if isinstance(api_url, str) and api_url.strip():
        return api_url.strip()
    return DEFAULT_API_URL
