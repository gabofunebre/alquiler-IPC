import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
from services import ipc_service


class LeerCsvTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.cache_path = os.path.join(self.tmpdir.name, "ipc.csv")
        self.meta_path = self.cache_path + ".meta"

        cache_patch = mock.patch.object(ipc_service, "CACHE_PATH", self.cache_path)
        meta_patch = mock.patch.object(ipc_service, "CACHE_METADATA_PATH", self.meta_path)
        meta_json_patch = mock.patch.object(ipc_service, "CACHE_META_PATH", self.meta_path)
        self.addCleanup(cache_patch.stop)
        self.addCleanup(meta_patch.stop)
        self.addCleanup(meta_json_patch.stop)
        cache_patch.start()
        meta_patch.start()
        meta_json_patch.start()

    def _write_cache(self, content):
        with open(self.cache_path, "w", encoding="utf-8") as f:
            f.write(content)

    def _age_cache(self):
        old = time.time() - (ipc_service.CACHE_TTL + 10)
        os.utime(self.cache_path, (old, old))

    def test_offline_uses_cached_data(self):
        csv_content = "fecha,valor\n2020-01-01,1.50\n"
        self._write_cache(csv_content)
        self._age_cache()

        with mock.patch.object(
            ipc_service.requests,
            "get",
            side_effect=requests.RequestException("offline"),
        ):
            header, rows, status = ipc_service.leer_csv()

        self.assertEqual(header, ["fecha", "valor"])
        self.assertEqual(rows, [["2020-01-01", "1.50"]])
        self.assertTrue(status["used_cache"])
        self.assertTrue(status["stale"])
        self.assertFalse(status["updated"])
        self.assertEqual(status["error"], "offline")

    def test_not_modified_reuses_cache_and_sends_conditionals(self):
        csv_content = "fecha,valor\n2020-01-01,1.50\n"
        self._write_cache(csv_content)
        self._age_cache()

        metadata = {
            "etag": "etag-value",
            "last_modified": "Wed, 01 May 2024 10:00:00 GMT",
        }
        with open(self.meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f)

        response = mock.Mock()
        response.status_code = 304
        response.headers = {
            "ETag": metadata["etag"],
            "Last-Modified": metadata["last_modified"],
        }
        response.raise_for_status.side_effect = AssertionError("304 should not raise")

        with mock.patch.object(ipc_service.requests, "get", return_value=response) as mocked_get:
            header, rows, status = ipc_service.leer_csv()

        self.assertEqual(header, ["fecha", "valor"])
        self.assertEqual(rows, [["2020-01-01", "1.50"]])
        self.assertTrue(status["used_cache"])
        self.assertFalse(status["updated"])
        self.assertFalse(status["stale"])
        self.assertIsNone(status["error"])
        mocked_get.assert_called_once()
        called_headers = mocked_get.call_args.kwargs.get("headers")
        self.assertIsNotNone(called_headers)
        self.assertEqual(called_headers.get("If-None-Match"), metadata["etag"])
        self.assertEqual(called_headers.get("If-Modified-Since"), metadata["last_modified"])
        self.assertIsNotNone(status["last_checked_at"])
        self.assertIsNotNone(status["last_cached_at"])
        with open(self.meta_path, "r", encoding="utf-8") as f:
            stored_metadata = json.load(f)
        self.assertEqual(stored_metadata.get("etag"), metadata["etag"])
        self.assertEqual(stored_metadata.get("last_modified"), metadata["last_modified"])


if __name__ == "__main__":
    unittest.main()
