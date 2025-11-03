import json
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone, date
from decimal import Decimal
from pathlib import Path
from typing import Any
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
        self.assertIn("unofficial_months", status)
        self.assertEqual(status["unofficial_months"], [])
        self.assertFalse(status.get("contains_unofficial"))
        self.assertIn("fallback_source", status)

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

        fallback_response = mock.Mock()
        fallback_response.status_code = 200
        fallback_response.headers = {}
        fallback_response.raise_for_status = mock.Mock()
        fallback_response.json.return_value = [
            {"fecha": "2024-06-01", "valor": 1.20}
        ]

        fallback_url = ipc_service.config_service.get_fallback_api_url()

        def fake_get(url, *args, **kwargs):
            if url == custom_url:
                return response
            self.assertEqual(url, fallback_url)
            return fallback_response

        with mock.patch.object(ipc_service.requests, "get", side_effect=fake_get) as mocked_get:
            header, rows, status = ipc_service.fetch_ipc_data()

        self.assertEqual(mocked_get.call_count, 2)
        called_url = mocked_get.call_args_list[0].args[0]
        self.assertEqual(called_url, custom_url)
        self.assertEqual(header, ["fecha", "variacion_mensual", "source"])
        self.assertEqual(rows, [["2024-05", "1.5", "official"], ["2024-06", "1.2", "backup"]])
        self.assertEqual(status["source"], custom_url)
        self.assertEqual(status["fallback_source"], fallback_url)
        self.assertTrue(status["used_backup"])
        self.assertTrue(status["contains_unofficial"])
        self.assertIn("2024-06", status["unofficial_months"])

    def test_primary_success_but_missing_required_month_uses_fallback_with_error(self):
        response = mock.Mock()
        response.status_code = 200
        response.headers = {}
        response.raise_for_status = mock.Mock()
        response.json.return_value = {"data": [["2024-05-01", "0.015"]]}

        fallback_rows = [["2024-06-01", "0.019", "backup"]]
        fallback_info = {"unofficial_months": ["2024-06"], "fallback_source": "https://fallback.example"}

        fallback_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

        def fake_fetch_backup_ipc(*args, **kwargs):
            fallback_calls.append((args, kwargs))
            return ["fecha", "variacion_mensual", "source"], fallback_rows, fallback_info

        required_key = ipc_service._month_key("2024-06")

        with mock.patch.object(ipc_service.requests, "get", return_value=response) as mocked_get, (
            mock.patch.object(ipc_service, "fetch_backup_ipc", side_effect=fake_fetch_backup_ipc)
        ) as mocked_backup, mock.patch.object(
            ipc_service, "_required_month_key", return_value=required_key
        ):
            header, rows, status = ipc_service.fetch_ipc_data()

        self.assertEqual(mocked_get.call_count, 1)
        self.assertEqual(mocked_backup.call_count, 1)
        self.assertEqual(len(fallback_calls), 1)
        first_call_args, first_call_kwargs = fallback_calls[0]
        self.assertFalse(first_call_args)
        self.assertFalse(first_call_kwargs)

        self.assertEqual(header, ["fecha", "variacion_mensual", "source"])
        self.assertEqual(
            rows,
            [["2024-05", "0.015", "official"], ["2024-06", "0.019", "backup"]],
        )
        self.assertTrue(status["used_backup"])
        self.assertFalse(status["stale"])
        self.assertIsInstance(status["error"], dict)
        self.assertEqual(status["error"].get("code"), "primary_stale")
        self.assertEqual(status["error"].get("message"), "La API principal no se actualizó")
        self.assertIn("2024-06", status["unofficial_months"])
        self.assertEqual(status["fallback_source"], fallback_info["fallback_source"])

    def test_primary_and_fallback_missing_required_month_preserves_error(self):
        response = mock.Mock()
        response.status_code = 200
        response.headers = {}
        response.raise_for_status = mock.Mock()
        response.json.return_value = {"data": [["2024-05-01", "0.015"]]}

        fallback_header = ["fecha", "variacion_mensual", "source"]
        fallback_rows = []
        fallback_info = {"unofficial_months": [], "fallback_source": "https://fallback.example"}

        fallback_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

        def fake_fetch_backup_ipc(*args, **kwargs):
            fallback_calls.append((args, kwargs))
            return fallback_header, fallback_rows, fallback_info

        required_key = ipc_service._month_key("2024-06")

        with mock.patch.object(ipc_service.requests, "get", return_value=response) as mocked_get, (
            mock.patch.object(ipc_service, "fetch_backup_ipc", side_effect=fake_fetch_backup_ipc)
        ) as mocked_backup, mock.patch.object(
            ipc_service, "_required_month_key", return_value=required_key
        ):
            header, rows, status = ipc_service.fetch_ipc_data()

        self.assertEqual(mocked_get.call_count, 1)
        self.assertEqual(mocked_backup.call_count, 1)
        self.assertEqual(len(fallback_calls), 1)
        first_call_args, first_call_kwargs = fallback_calls[0]
        self.assertFalse(first_call_args)
        self.assertFalse(first_call_kwargs)
        self.assertEqual(header, ["fecha", "variacion_mensual", "source"])
        self.assertEqual(rows, [["2024-05", "0.015", "official"]])
        self.assertFalse(status["used_backup"])
        self.assertTrue(status["stale"])
        self.assertIsInstance(status["error"], dict)
        self.assertEqual(status["error"].get("code"), "primary_stale")
        self.assertEqual(status["error"].get("message"), "La API principal no se actualizó")
        self.assertEqual(status["unofficial_months"], [])
        self.assertEqual(status["fallback_source"], fallback_info["fallback_source"])

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

    def test_backup_used_when_primary_request_fails(self):
        backup_rows = [["2024-07-01", "0.021"], ["2024-08-01", "0.022"]]
        backup_info = {"unofficial_months": ["2024-07", "2024-08"]}

        with mock.patch.object(
            ipc_service.requests,
            "get",
            side_effect=requests.RequestException("timeout"),
        ), mock.patch.object(
            ipc_service,
            "fetch_backup_ipc",
            return_value=(["fecha", "variacion_mensual"], backup_rows, backup_info),
            create=True,
        ):
            header, rows, status = ipc_service.fetch_ipc_data()

        self.assertEqual(header, ["fecha", "variacion_mensual"])
        self.assertEqual(rows, backup_rows)
        self.assertTrue(status["used_backup"])
        self.assertTrue(status["contains_unofficial"])
        self.assertEqual(status["unofficial_months"], ["2024-07", "2024-08"])

        with open(self.cache_path, "r", encoding="utf-8") as fh:
            cached_payload = json.load(fh)

        self.assertEqual(cached_payload.get("rows"), backup_rows)

        with open(self.meta_path, "r", encoding="utf-8") as fh:
            meta = json.load(fh)

        self.assertEqual(meta.get("unofficial_months"), ["2024-07", "2024-08"])

    def test_official_update_clears_unofficial_metadata(self):
        backup_rows = [["2024-07-01", "0.021"], ["2024-08-01", "0.022"]]
        backup_info = {"unofficial_months": ["2024-07", "2024-08"]}

        success_response = mock.Mock()
        success_response.status_code = 200
        success_response.headers = {}
        success_response.raise_for_status = mock.Mock()
        success_response.json.return_value = {
            "data": [
                ["2024-07-01", "0.021"],
                ["2024-08-01", "0.022"],
                ["2024-09-01", "0.023"],
            ]
        }

        request_side_effects = [
            requests.RequestException("boom"),
            success_response,
        ]

        backup_side_effects = [
            (["fecha", "variacion_mensual"], backup_rows, backup_info),
            AssertionError("fetch_backup_ipc should not be called on success"),
        ]

        with mock.patch.object(
            ipc_service.requests, "get", side_effect=request_side_effects
        ) as mocked_get, mock.patch.object(
            ipc_service,
            "fetch_backup_ipc",
            side_effect=backup_side_effects,
            create=True,
        ) as mocked_backup:
            header_backup, rows_backup, status_backup = ipc_service.fetch_ipc_data()
            header_success, rows_success, status_success = ipc_service.fetch_ipc_data()

        self.assertEqual(header_backup, ["fecha", "variacion_mensual"])
        self.assertEqual(rows_backup, backup_rows)
        self.assertTrue(status_backup["used_backup"])
        self.assertTrue(status_backup["contains_unofficial"])
        self.assertEqual(status_backup["unofficial_months"], ["2024-07", "2024-08"])

        self.assertEqual(header_success, ["fecha", "variacion_mensual", "source"])
        self.assertEqual(
            rows_success,
            [
                ["2024-07", "0.021", "official"],
                ["2024-08", "0.022", "official"],
                ["2024-09", "0.023", "official"],
            ],
        )
        self.assertFalse(status_success["used_backup"])
        self.assertFalse(status_success["contains_unofficial"])
        self.assertEqual(status_success["unofficial_months"], [])

        self.assertEqual(mocked_get.call_count, 2)
        self.assertEqual(mocked_backup.call_count, 1)

        with open(self.meta_path, "r", encoding="utf-8") as fh:
            meta = json.load(fh)

        self.assertNotIn("unofficial_months", meta)

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

    def test_rows_to_variations_ignore_extra_columns(self):
        rows = [["2024-05-01", "0.015", "backup"], ["2024-06-01", "0.016", "oficial"]]
        parsed = ipc_service._rows_to_monthly_variations(rows)
        self.assertEqual(parsed["2024-05"], Decimal("0.015"))
        self.assertEqual(parsed["2024-06"], Decimal("0.016"))

if __name__ == "__main__":
    unittest.main()
