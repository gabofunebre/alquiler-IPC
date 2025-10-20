import json
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone, date
from decimal import Decimal
from pathlib import Path
from unittest import mock

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
from services import ipc_service


class FetchIpcDataTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.cache_path = os.path.join(self.tmpdir.name, "ipc.json")
        self.meta_path = os.path.join(self.tmpdir.name, "ipc.meta.json")
        self.config_path = os.path.join(self.tmpdir.name, "config.json")

        cache_patch = mock.patch.object(ipc_service, "CACHE_PATH", self.cache_path)
        meta_json_patch = mock.patch.object(ipc_service, "CACHE_META_PATH", self.meta_path)
        config_patch = mock.patch("services.config_service.CONFIG_FILE", self.config_path)
        self.addCleanup(cache_patch.stop)
        self.addCleanup(meta_json_patch.stop)
        self.addCleanup(config_patch.stop)
        cache_patch.start()
        meta_json_patch.start()
        config_patch.start()

    def _write_cache(self, rows):
        payload = {"header": ["fecha", "variacion_mensual"], "rows": rows}
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(payload, f)

    def _age_cache(self):
        old = time.time() - (24 * 60 * 60 + 10)
        os.utime(self.cache_path, (old, old))

    def test_offline_uses_cached_data(self):
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        rows = [[f"{current_month}-01", "1.50"]]
        self._write_cache(rows)
        self._age_cache()

        with mock.patch.object(
            ipc_service.requests,
            "get",
            side_effect=requests.RequestException("offline"),
        ):
            header, rows, status = ipc_service.fetch_ipc_data()

        self.assertEqual(header, ["fecha", "variacion_mensual"])
        self.assertEqual(rows, [[f"{current_month}-01", "1.50"]])
        self.assertTrue(status["used_cache"])
        self.assertFalse(status["stale"])

    def test_not_modified_reuses_cache_and_sends_conditionals(self):
        rows = [["2024-02-01", "1.50"]]
        self._write_cache(rows)
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

        original_is_cache_stale = ipc_service._is_cache_stale

        def fake_is_cache_stale(latest_month, *, today=None):
            return original_is_cache_stale(latest_month, today=date(2024, 4, 14))

        with mock.patch.object(ipc_service, "_is_cache_stale", fake_is_cache_stale):
            with mock.patch.object(ipc_service.requests, "get", return_value=response) as mocked_get:
                header, rows, status = ipc_service.fetch_ipc_data()

        self.assertEqual(header, ["fecha", "variacion_mensual"])
        self.assertEqual(rows, [["2024-02-01", "1.50"]])
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
        self.assertTrue(status["used_cache"])
        self.assertTrue(status["stale"])

    def test_refresh_triggered_on_14th_when_previous_month_missing(self):
        rows = [["2024-02-01", "1.50"]]
        self._write_cache(rows)

        original_is_cache_stale = ipc_service._is_cache_stale

        def fake_is_cache_stale(latest_month, *, today=None):
            return original_is_cache_stale(latest_month, today=date(2024, 4, 14))

        with mock.patch.object(ipc_service, "_is_cache_stale", fake_is_cache_stale):
            with mock.patch.object(
                ipc_service.requests,
                "get",
                side_effect=requests.RequestException("offline"),
            ) as mocked_get:
                header, rows, status = ipc_service.fetch_ipc_data()

        mocked_get.assert_called_once()
        self.assertEqual(header, ["fecha", "variacion_mensual"])
        self.assertEqual(rows, [["2024-02-01", "1.50"]])
        self.assertTrue(status["used_cache"])
        self.assertTrue(status["stale"])
        self.assertIsInstance(status["error"], dict)
        self.assertEqual(status["error"].get("code"), "request_error")

    def test_cache_considered_fresh_before_14th_with_two_month_gap(self):
        rows = [["2024-02-01", "1.50"]]
        self._write_cache(rows)

        original_is_cache_stale = ipc_service._is_cache_stale

        def fake_is_cache_stale(latest_month, *, today=None):
            return original_is_cache_stale(latest_month, today=date(2024, 4, 13))

        with mock.patch.object(ipc_service, "_is_cache_stale", fake_is_cache_stale):
            with mock.patch.object(ipc_service.requests, "get") as mocked_get:
                header, rows, status = ipc_service.fetch_ipc_data()

        mocked_get.assert_not_called()
        self.assertEqual(header, ["fecha", "variacion_mensual"])
        self.assertEqual(rows, [["2024-02-01", "1.50"]])
        self.assertTrue(status["used_cache"])
        self.assertFalse(status["stale"])

    def test_fetch_ipc_data_uses_configured_url(self):
        custom_url = "https://example.com/custom-ipc.json"
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump({"api_url": custom_url}, f)

        response = mock.Mock()
        response.status_code = 200
        response.headers = {}
        response.raise_for_status = mock.Mock()
        response.json.return_value = {"data": [["2024-05-01", 1.50]]}

        with mock.patch.object(ipc_service.requests, "get", return_value=response) as mocked_get:
            header, rows, status = ipc_service.fetch_ipc_data()

        mocked_get.assert_called_once()
        called_url = mocked_get.call_args.args[0]
        self.assertEqual(called_url, custom_url)
        self.assertEqual(header, ["fecha", "variacion_mensual"])
        self.assertEqual(rows, [["2024-05-01", "1.5"]])
        self.assertEqual(status["source"], custom_url)

    def test_invalid_api_response_uses_cache(self):
        rows = [["2024-05-01", "1.50"]]
        self._write_cache(rows)
        self._age_cache()

        response = mock.Mock()
        response.status_code = 200
        response.raise_for_status = mock.Mock()
        response.json.side_effect = ValueError("invalid json")

        with mock.patch.object(ipc_service.requests, "get", return_value=response):
            header, cached_rows, status = ipc_service.fetch_ipc_data()

        self.assertEqual(header, ["fecha", "variacion_mensual"])
        self.assertEqual(cached_rows, rows)
        self.assertTrue(status["used_cache"])
        self.assertIsInstance(status["error"], dict)
        self.assertEqual(status["error"].get("code"), "invalid_response")

    def test_get_cache_status_without_cache(self):
        info = ipc_service.get_cache_status()
        self.assertFalse(info["has_cache"])
        self.assertIsNone(info["last_cached_at"])
        self.assertIsNone(info["last_cached_at_text"])
        self.assertIsNone(info["fetched_at"])
        self.assertIsNone(info["fetched_at_text"])

    def test_get_cache_status_with_cache(self):
        timestamp = datetime(2024, 5, 10, 12, 30, tzinfo=timezone.utc)
        rows = [["2024-05-01", "1.50"]]
        self._write_cache(rows)
        os.utime(self.cache_path, (timestamp.timestamp(), timestamp.timestamp()))
        meta = {"fetched_at": "2024-05-09T10:00:00+00:00"}
        with open(self.meta_path, "w", encoding="utf-8") as fh:
            json.dump(meta, fh)

        info = ipc_service.get_cache_status()

        self.assertTrue(info["has_cache"])
        self.assertEqual(info["last_cached_at"], timestamp)
        self.assertIsInstance(info["last_cached_at_text"], str)
        expected_fetched = datetime(2024, 5, 9, 10, 0, tzinfo=timezone.utc)
        self.assertEqual(info["fetched_at"], expected_fetched)
        self.assertIsInstance(info["fetched_at_text"], str)


class CacheFreshnessRuleTests(unittest.TestCase):
    def test_is_cache_stale_on_14th_without_previous_month(self):
        self.assertTrue(
            ipc_service._is_cache_stale("2024-02", today=date(2024, 4, 14))
        )

    def test_is_cache_fresh_on_14th_with_previous_month(self):
        self.assertFalse(
            ipc_service._is_cache_stale("2024-03", today=date(2024, 4, 14))
        )


class ParsingTests(unittest.TestCase):
    def test_rows_to_variations_from_api_data(self):
        rows = [["2024-05-01", "0.015"], ["2024-06-01", "0.016"]]
        parsed = ipc_service._rows_to_monthly_variations(rows)
        self.assertIn("2024-05", parsed)
        self.assertEqual(parsed["2024-05"], Decimal("0.015"))
        self.assertIn("2024-06", parsed)
        self.assertEqual(parsed["2024-06"], Decimal("0.016"))

if __name__ == "__main__":
    unittest.main()
