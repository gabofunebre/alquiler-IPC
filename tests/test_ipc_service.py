import csv
import os
import tempfile
import time
import unittest
from unittest import mock

import requests

from services import ipc_service


class LeerCsvTests(unittest.TestCase):
    def test_leer_csv_uses_stale_cache_when_download_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = os.path.join(tmpdir, "ipc.csv")
            header = ["fecha", "indice"]
            cached_row = ["2000-01-01", "1.0"]
            with open(cache_path, "w", encoding="utf-8", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(header)
                writer.writerow(cached_row)

            # Hacer que el archivo parezca más antiguo que el TTL para forzar la descarga
            old_time = time.time() - (ipc_service.CACHE_TTL + 5)
            os.utime(cache_path, (old_time, old_time))

            success_response = mock.Mock()
            success_response.content = "fecha,indice\n2021-01-01,2.0\n".encode("utf-8")
            success_response.raise_for_status = mock.Mock()

            with mock.patch.object(ipc_service, "CACHE_PATH", cache_path):
                with mock.patch(
                    "services.ipc_service.requests.get",
                    side_effect=[requests.RequestException("boom"), success_response],
                ) as mock_get:
                    with self.assertLogs("services.ipc_service", level="WARNING") as logs:
                        header1, rows1 = ipc_service.leer_csv()

                    header2, rows2 = ipc_service.leer_csv()

            self.assertEqual(header1, header)
            self.assertEqual(rows1, [cached_row])
            self.assertEqual(mock_get.call_count, 2)
            success_response.raise_for_status.assert_called_once()
            self.assertEqual(rows2, [["2021-01-01", "2.0"]])
            self.assertTrue(
                any("Falling back to cached IPC CSV" in message for message in logs.output)
            )

    def test_leer_csv_raises_when_no_cache_available(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = os.path.join(tmpdir, "ipc.csv")

            with mock.patch.object(ipc_service, "CACHE_PATH", cache_path):
                with mock.patch(
                    "services.ipc_service.requests.get",
                    side_effect=requests.RequestException("boom"),
                ):
                    with self.assertRaises(requests.RequestException):
                        ipc_service.leer_csv()


if __name__ == "__main__":
    unittest.main()
