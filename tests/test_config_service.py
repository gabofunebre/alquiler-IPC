import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services import config_service


class GetCsvUrlTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

        self.config_path = os.path.join(self.tmpdir.name, "config.json")
        patcher = mock.patch.object(config_service, "CONFIG_FILE", self.config_path)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_returns_default_when_not_configured(self):
        self.assertEqual(
            config_service.get_csv_url(), config_service.DEFAULT_CSV_URL
        )

    def test_trims_value_and_persists_sanitized_copy(self):
        original_value = "  https://example.com/ipc.csv  "
        config_service.save_config({"csv_url": original_value})

        self.assertEqual(
            config_service.get_csv_url(), "https://example.com/ipc.csv"
        )

        with open(self.config_path, "r", encoding="utf-8") as fh:
            stored = json.load(fh)
        self.assertEqual(stored.get("csv_url"), "https://example.com/ipc.csv")

    def test_invalid_value_falls_back_to_default(self):
        config_service.save_config({"csv_url": 123})

        self.assertEqual(
            config_service.get_csv_url(), config_service.DEFAULT_CSV_URL
        )
        self.assertEqual(config_service.load_config(), {})


if __name__ == "__main__":
    unittest.main()
