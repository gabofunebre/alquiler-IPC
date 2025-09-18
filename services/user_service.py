import json
import os
from copy import deepcopy
from typing import Any, Dict

USERS_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "users.json")

USER_CONFIG_KEYS = (
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
)

LEGACY_KEY_MAP = {
    "alquiler_base": "valor_inicial_contrato",
}


def _normalize_username(name: str | None) -> str:
    if not name:
        return ""
    return str(name).strip().lower()


def _default_user_config() -> Dict[str, Any]:
    return {key: "" for key in USER_CONFIG_KEYS}


def _sanitize_user_config(data: Any, *, base: Dict[str, Any] | None = None) -> Dict[str, Any]:
    base_config = deepcopy(base) if base is not None else _default_user_config()
    extras: Dict[str, Any] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            target_key = LEGACY_KEY_MAP.get(key, key)
            if target_key in base_config:
                base_config[target_key] = "" if value is None else value
            elif target_key == key:
                extras[key] = value
    base_config.update(extras)
    return base_config


def _read_users_file() -> Any:
    path = os.path.abspath(USERS_FILE)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {}


def _write_users_file(data: Dict[str, Any]) -> None:
    path = os.path.abspath(USERS_FILE)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, sort_keys=True)


def _normalize_users_data(raw: Any) -> tuple[Dict[str, Dict[str, Any]], bool]:
    changed = False
    users: Dict[str, Dict[str, Any]] = {}

    if isinstance(raw, list):
        changed = True
        for item in raw:
            username = _normalize_username(item)
            if not username:
                continue
            users[username] = _default_user_config()
        return users, changed

    if isinstance(raw, dict):
        if not raw:
            return {}, False
        # Old global configuration stored here – drop it and start fresh
        if set(raw.keys()).issubset(USER_CONFIG_KEYS):
            return {}, True
        for username_raw, user_data in raw.items():
            username = _normalize_username(username_raw)
            if not username:
                changed = True
                continue
            sanitized = _sanitize_user_config(user_data)
            users[username] = sanitized
            if sanitized != user_data:
                changed = True
        return users, changed

    if raw in ({}, None):
        return {}, False

    return {}, True


def load_users() -> Dict[str, Dict[str, Any]]:
    """Load users and their configuration from JSON file."""

    raw = _read_users_file()
    users, changed = _normalize_users_data(raw)
    if changed:
        _write_users_file(users)
    return users


def save_users(users: Dict[str, Dict[str, Any]]) -> None:
    """Persist the full user configuration mapping to JSON."""

    sanitized, _ = _normalize_users_data(users)
    _write_users_file(sanitized)


def list_users() -> list[str]:
    """Return the list of registered users sorted alphabetically."""

    return sorted(load_users().keys())


def add_user(name: str) -> str | None:
    """Add a user with default configuration. Returns normalized username."""

    username = _normalize_username(name)
    if not username:
        return None
    users = load_users()
    if username not in users:
        users[username] = _default_user_config()
        save_users(users)
    return username


def delete_user(name: str) -> None:
    """Remove a user (case insensitive)."""

    username = _normalize_username(name)
    if not username:
        return
    users = load_users()
    if username in users:
        del users[username]
        save_users(users)


def get_user_config(name: str | None) -> Dict[str, Any]:
    """Retrieve configuration for the given user or defaults if missing."""

    username = _normalize_username(name)
    if not username:
        return _default_user_config()
    users = load_users()
    config = users.get(username)
    if config is None:
        return _default_user_config()
    return _sanitize_user_config(config)


def save_user_config(name: str, updates: Dict[str, Any]) -> None:
    """Persist user configuration merging with existing data."""

    username = _normalize_username(name)
    if not username:
        return
    users = load_users()
    current = users.get(username, _default_user_config())
    merged_input: Dict[str, Any] = {}
    if isinstance(updates, dict):
        merged_input.update(current)
        merged_input.update(updates)
    else:
        merged_input = current
    users[username] = _sanitize_user_config(merged_input)
    save_users(users)
