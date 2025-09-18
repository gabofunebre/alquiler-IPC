import json
import os
from typing import Any, Dict

from dotenv import load_dotenv

load_dotenv()

DEFAULT_CSV_URL = os.getenv(
    "CSV_URL",
    os.getenv(
        "CSV_DATOS",
        "https://infra.datos.gob.ar/catalog/sspm/dataset/145/distribution/145.3/download/indice-precios-al-consumidor-nivel-general-base-diciembre-2016-mensual.csv",
    ),
)
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")

USER_CONFIG_KEYS = {"alquiler_base", "fecha_inicio_contrato", "periodo_actualizacion_meses"}


def _sanitize_global_config(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    sanitized = {k: v for k, v in data.items() if k not in USER_CONFIG_KEYS}
    return sanitized


def _write_config(data: Dict[str, Any]) -> None:
    path = os.path.abspath(CONFIG_FILE)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    to_store: Any
    if isinstance(data, dict):
        to_store = data.copy()
        csv_url_value = to_store.get("csv_url")
        if isinstance(csv_url_value, str):
            to_store["csv_url"] = csv_url_value.strip()
    else:
        to_store = data
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, sort_keys=True)


def load_config() -> Dict[str, Any]:
    """Load global configuration from JSON file."""

    path = os.path.abspath(CONFIG_FILE)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {}
    sanitized = _sanitize_global_config(raw)
    if sanitized != raw:
        _write_config(sanitized)
    return sanitized


def save_config(data: Dict[str, Any]) -> None:
    """Persist global configuration to JSON file."""

    sanitized = _sanitize_global_config(data)
    _write_config(sanitized)
