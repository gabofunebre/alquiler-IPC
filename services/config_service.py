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

_DEFAULT_CONFIG: Dict[str, Any] = {
    "alquiler_base": "",
    "fecha_inicio_contrato": "",
    "periodo_actualizacion_meses": "",
    "csv_url": "",
}


def _config_path() -> str:
    return os.path.abspath(CONFIG_FILE)


def _read_raw_config() -> Dict[str, Any]:
    """Return the raw config from disk without applying defaults."""

    path = _config_path()
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        return {}
    return data


def load_config() -> Dict[str, Any]:
    """Load configuration applying defaults for missing values."""

    data = _DEFAULT_CONFIG.copy()
    raw = _read_raw_config()
    data.update(raw)
    csv_url = raw.get("csv_url") if isinstance(raw, dict) else ""
    if isinstance(csv_url, str):
        csv_url = csv_url.strip()
    elif csv_url is not None:
        csv_url = str(csv_url)
    else:
        csv_url = ""
    if csv_url:
        data["csv_url"] = csv_url
    else:
        data["csv_url"] = DEFAULT_CSV_URL
    return data


def get_csv_url() -> str:
    """Return the configured CSV URL falling back to environment defaults."""

    raw = _read_raw_config()
    csv_url = raw.get("csv_url") if isinstance(raw, dict) else ""
    if isinstance(csv_url, str):
        csv_url = csv_url.strip()
    elif csv_url is not None:
        csv_url = str(csv_url)
    else:
        csv_url = ""
    if csv_url:
        return csv_url
    return DEFAULT_CSV_URL


def save_config(data: Dict[str, Any]) -> None:
    """Persist configuration to JSON file."""

    path = _config_path()
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
        json.dump(to_store, fh)
