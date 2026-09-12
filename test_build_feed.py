import csv
import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import build_feed as feed

NOW = int(datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)


def row(**changes):
    return {
        "latitude": "22", "longitude": "18", "acq_date": "2026-09-06", "acq_time": "1100",
        "satellite": "N20", "instrument": "VIIRS", "confidence": "n", "frp": "50",
    } | changes


def payload(*rows):
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=sorted(feed.FIELDS))
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode()


def all_payloads():
    return {source: payload(row(satellite=code)) for source, (code, _) in feed.SOURCES.items()}


class FeedTest(unittest.TestCase):
    def test_filters_on_job_before_output_and_keeps_exact_threshold(self):
        points, _ = feed.parse_csv(payload(row(), row(frp="49.99"), row(confidence="l")), "VIIRS_NOAA20_NRT", NOW)
        self.assertEqual([50.0], [point["frp"] for point in points])

    def test_utc_window_and_midnight_time(self):
        points, latest = feed.parse_csv(payload(
            row(acq_date="2026-09-05", acq_time="1200"),
            row(acq_date="2026-09-05", acq_time="1159"), row(acq_time="1"), row(),
        ), "VIIRS_NOAA20_NRT", NOW)
        self.assertEqual(3, len(points))
        self.assertEqual(NOW - 60 * 60 * 1000, latest)

    def test_refuses_malformed_low_quality_and_stale_sources(self):
        for candidate in [row(latitude="91"), row(longitude="-190"), row(frp="nan"),
                          row(frp="inf"), row(frp="-1"), row(acq_time="2460"),
                          row(acq_time="1.5"), row(acq_date="2026-09-07"),
                          row(satellite="N21"), row(instrument="MODIS"), row(confidence="x"),
                          row(acq_date="2026-09-04")]:
            with self.subTest(candidate=candidate), self.assertRaises(ValueError):
                feed.parse_csv(payload(candidate), "VIIRS_NOAA20_NRT", NOW)
        for content in (b"<html>Error</html>", payload(), b"x" * (feed.MAX_INPUT_BYTES + 1)):
            with self.assertRaises(ValueError):
                feed.parse_csv(content, "VIIRS_NOAA20_NRT", NOW)

    def test_selection_preserves_sparse_regions_and_is_unique_deterministic_and_bounded(self):
        dense = [dict(lat=22.0, lon=18 + i / 10000, time=NOW, frp=1000.0 + i,
                      confidence="h", satellite="NOAA-20 VIIRS") for i in range(1200)]
        sparse = dict(lat=-40.0, lon=-100.0, time=NOW, frp=50.0, confidence="n", satellite="NOAA-21 VIIRS")
        selected, count = feed.select(dense + dense + [sparse])
        self.assertEqual(1201, count)
        self.assertEqual(feed.MAX_HOTSPOTS, len(selected))
        self.assertIn(sparse, selected)
        self.assertEqual((selected, count), feed.select(list(reversed(dense + dense + [sparse]))))
        self.assertEqual(len(selected), len({feed.identity(point) for point in selected}))

    def test_output_contract_requires_all_satellites_and_allows_zero_qualifying_points(self):
        inputs = all_payloads()
        document = json.loads(feed.build(inputs, NOW))
        self.assertEqual(1, document["schemaVersion"])
        self.assertEqual("world", document["coverage"])
        self.assertEqual(NOW, document["generatedAtMillis"])
        self.assertEqual(set(feed.SOURCES), set(document["sourceLatestMillis"]))
        self.assertEqual(2, len(document["hotspots"]))
        self.assertFalse(document["limited"])
        self.assertLess(len(feed.build(inputs, NOW)), feed.MAX_OUTPUT_BYTES)
        with self.assertRaises(ValueError):
            feed.build({"VIIRS_NOAA20_NRT": inputs["VIIRS_NOAA20_NRT"]}, NOW)
        weak = {source: payload(row(satellite=code, frp="3")) for source, (code, _) in feed.SOURCES.items()}
        self.assertEqual([], json.loads(feed.build(weak, NOW))["hotspots"])

    def test_failed_refresh_preserves_the_previous_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "feed.json"
            path.write_text("previous valid feed")
            with patch("sys.argv", ["build_feed.py", "--output", str(path)]), patch.dict("os.environ", {"FIRMS_MAP_KEY": ""}):
                self.assertEqual(1, feed.main())
            self.assertEqual("previous valid feed", path.read_text())

    def test_transient_download_failure_retries_but_authentication_failure_does_not(self):
        import urllib.error
        secret = "a" * 32
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"csv fixture"
        opener = MagicMock()
        opener.open.side_effect = [urllib.error.HTTPError(secret, 503, "unavailable", {}, None), response]
        with patch("urllib.request.build_opener", return_value=opener), patch("time.sleep") as sleep:
            self.assertEqual(b"csv fixture", feed.download(secret, "VIIRS_NOAA20_NRT"))
            self.assertEqual(2, opener.open.call_count)
            sleep.assert_called_once_with(2)
        opener.open.reset_mock()
        opener.open.side_effect = urllib.error.HTTPError(secret, 403, "forbidden", {}, None)
        with patch("urllib.request.build_opener", return_value=opener), patch("time.sleep") as sleep:
            with self.assertRaises(ValueError): feed.download(secret, "VIIRS_NOAA20_NRT")
            self.assertEqual(1, opener.open.call_count)
            sleep.assert_not_called()

    def test_diagnostics_allow_known_reasons_without_exposing_untrusted_messages(self):
        self.assertEqual("NASA CSV schema missing", feed.safe_failure_reason(ValueError("NASA CSV schema missing")))
        secret = "a" * 32
        self.assertNotIn(secret, feed.safe_failure_reason(ValueError("Request failed: " + secret)))

    def test_timeout_retries_are_bounded_and_report_a_safe_reason(self):
        import urllib.error
        secret = "a" * 32
        for error in (TimeoutError(secret), urllib.error.URLError(TimeoutError(secret))):
            opener = MagicMock()
            opener.open.side_effect = error
            with self.subTest(error=type(error).__name__), patch(
                "urllib.request.build_opener", return_value=opener
            ), patch("time.sleep") as sleep:
                with self.assertRaises(ValueError) as result:
                    feed.download(secret, "VIIRS_NOAA20_NRT")
                self.assertEqual(feed.MAX_DOWNLOAD_ATTEMPTS, opener.open.call_count)
                self.assertEqual([2, 4, 8, 16], [call.args[0] for call in sleep.call_args_list])
                self.assertEqual(
                    "NASA download timed out for VIIRS_NOAA20_NRT", feed.safe_failure_reason(result.exception)
                )
                self.assertNotIn(secret, str(result.exception))

    def test_exhausted_network_errors_keep_only_the_source_and_failure_category(self):
        import urllib.error
        secret = "a" * 32
        for error, category in (
            (urllib.error.HTTPError(secret, 503, secret, {}, None), "HTTP request failed"),
            (urllib.error.URLError(secret), "connection failed"),
        ):
            opener = MagicMock()
            opener.open.side_effect = error
            with self.subTest(category=category), patch(
                "urllib.request.build_opener", return_value=opener
            ), patch("time.sleep"):
                with self.assertRaises(ValueError) as result:
                    feed.download(secret, "VIIRS_NOAA21_NRT")
                self.assertEqual(feed.MAX_DOWNLOAD_ATTEMPTS, opener.open.call_count)
                self.assertEqual(
                    f"NASA {category} for VIIRS_NOAA21_NRT", feed.safe_failure_reason(result.exception)
                )
                self.assertNotIn(secret, str(result.exception))

    def test_invalid_rows_are_skipped_when_the_source_still_has_fresh_data(self):
        points, latest = feed.parse_csv(payload(
            row(),
            row(latitude="91"),
            row(longitude="-190"),
            row(frp="-1"),
            row(frp="nan"),
            row(confidence="nominal", frp="80", latitude="-40", longitude="170"),
            row(acq_time="2460"),
            row(satellite="N21"),
        ), "VIIRS_NOAA20_NRT", NOW)
        self.assertEqual([50.0, 80.0], [point["frp"] for point in points])
        self.assertEqual("n", points[1]["confidence"])
        self.assertEqual(-40.0, points[1]["lat"])
        self.assertEqual(NOW - 60 * 60 * 1000, latest)

    def test_dateline_longitude_is_wrapped_instead_of_failing_the_refresh(self):
        points, _ = feed.parse_csv(payload(row(longitude="180.2", frp="60")), "VIIRS_NOAA20_NRT", NOW)
        self.assertEqual(1, len(points))
        self.assertAlmostEqual(-179.8, points[0]["lon"], places=4)

    def test_network_errors_do_not_expose_the_key(self):
        secret = "a" * 32
        with patch("urllib.request.build_opener", side_effect=RuntimeError(secret)):
            with self.assertRaises(ValueError) as result:
                feed.download(secret, "VIIRS_NOAA20_NRT")
        self.assertNotIn(secret, str(result.exception))

    def test_download_all_retries_only_the_failed_source(self):
        secret = "a" * 32
        n20, n21 = b"n20-csv", b"n21-csv"
        state = {"n20": 0}

        def fake_download(_key, source):
            if source == "VIIRS_NOAA20_NRT":
                state["n20"] += 1
                if state["n20"] == 1:
                    raise ValueError("NASA connection failed for VIIRS_NOAA20_NRT")
                return n20
            return n21

        with patch.object(feed, "download", side_effect=fake_download), patch("time.sleep") as slept:
            payloads = feed.download_all(secret)
        self.assertEqual({"VIIRS_NOAA20_NRT": n20, "VIIRS_NOAA21_NRT": n21}, payloads)
        self.assertEqual(2, state["n20"])
        slept.assert_called_once_with(feed.SOURCE_RETRY_DELAY_SECONDS)


if __name__ == "__main__":
    unittest.main()
