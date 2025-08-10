import os
import json

USERS_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "users.json")


def load_users():
    """Load list of allowed users from JSON file."""
    path = os.path.abspath(USERS_FILE)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    return [str(u).lower() for u in data]
        except (json.JSONDecodeError, OSError):
            return []
    return []


def save_users(users):
    """Persist user list to JSON file."""
    path = os.path.abspath(USERS_FILE)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(sorted({u.lower() for u in users}), fh)


def add_user(name):
    """Add a user to the list (case insensitive)."""
    if not name:
        return
    users = load_users()
    name_l = name.lower()
    if name_l not in users:
        users.append(name_l)
        save_users(users)


def delete_user(name):
    """Remove a user from the list (case insensitive)."""
    if not name:
        return
    users = load_users()
    name_l = name.lower()
    if name_l in users:
        users.remove(name_l)
        save_users(users)
