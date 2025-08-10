import os
import json
from dotenv import load_dotenv

load_dotenv()

CSV_URL = os.getenv(
    "CSV_URL",
    "https://infra.datos.gob.ar/catalog/sspm/dataset/145/distribution/145.3/download/indice-precios-al-consumidor-nivel-general-base-diciembre-2016-mensual.csv",
)
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")


def load_config():
    """Load configuration from JSON file."""
    path = os.path.abspath(CONFIG_FILE)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {
        "alquiler_base": "",
        "fecha_inicio_contrato": "",
        "periodo_actualizacion_meses": "",
    }


def save_config(data):
    """Persist configuration to JSON file."""
    path = os.path.abspath(CONFIG_FILE)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
