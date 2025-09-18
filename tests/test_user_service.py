import pytest

from services import user_service


@pytest.fixture
def users_file(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    file_path = config_dir / "users.json"
    monkeypatch.setattr(user_service, "USERS_FILE", str(file_path))
    return file_path


def test_find_user_by_name_matches_configured_name_case_insensitive(users_file):
    user_service.save_users(
        {
            "Juan Perez": {
                "nombre": "Juan",
                "apellido": "Pérez",
            },
            "josé": {
                "nombre": "José",
            },
        }
    )

    assert user_service.find_user_by_name("juan") == "juan perez"
    assert user_service.find_user_by_name("JUAN") == "juan perez"
    assert user_service.find_user_by_name("Juan Pérez") == "juan perez"
    assert user_service.find_user_by_name("Jose") == "josé"
    assert user_service.find_user_by_name("josé") == "josé"


def test_find_user_by_name_falls_back_to_identifier(users_file):
    user_service.save_users({"Contrato-123": {}})

    assert user_service.find_user_by_name("contrato-123") == "contrato-123"
    assert user_service.find_user_by_name("CONTRATO-123") == "contrato-123"
    assert user_service.find_user_by_name("desconocido") is None
    assert user_service.find_user_by_name("   ") is None
    assert user_service.find_user_by_name(None) is None
