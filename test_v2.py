"""Schema 2 regressions with small synthetic CSVs; no upstream network access."""

import contextlib
import csv
import io
import itertools
import json
import os
import subprocess
import tempfile
import threading
import sys
from datetime import datetime, timezone
import unittest
from pathlib import Path
from unittest.mock import patch

import build_feed as feed
from test_build_feed import NOW, all_payloads, payload, row

N20, N21 = tuple(feed.SOURCES)
TERRA, AQUA = feed.MODIS_SOURCES
KEY = "a" * 32  # synthetic, never an actual NASA credential


def modis_row(**changes):
    return row(satellite="T", instrument="MODIS", confidence="80") | {"version": "6.1NRT"} | changes


def modis_payload(*rows):
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=sorted(feed.MODIS_FIELDS))
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode()


def inputs():
    return all_payloads() | {"MODIS_NRT": modis_payload(modis_row(), modis_row(satellite="A"))}


def document(data, errors=None, now=NOW):
    return json.loads(feed.build_v2(data, now, errors))


class SchemaV2Test(unittest.TestCase):
    def assert_contract(self, result):
        self.assertEqual(2, result["schemaVersion"])
        self.assertEqual("world", result["coverage"])
        self.assertEqual(NOW, result["generatedAtMillis"])
        self.assertEqual(150, result["minFrpMw"])
        self.assertEqual(24, result["windowHours"])
        self.assertEqual(set(feed.SOURCE_PROFILES), set(result["sources"]))
        active = set(result["sourceLatestMillis"])
        for source, health in result["sources"].items():
            product, _, satellite, instrument = feed.SOURCE_PROFILES[source]
            self.assertEqual((product, satellite, instrument), (health["product"], health["satellite"], health["instrument"]))
            self.assertIs(type(health["selected"]), bool)
            self.assertEqual(source in active, health["selected"])
            if source in active:
                self.assertEqual("fresh", health["status"])
                self.assertEqual(result["sourceLatestMillis"][source], health["latestMillis"])
            if health["status"] in ("unavailable", "invalid"):
                self.assertIsNone(health["latestMillis"])
            else:
                self.assertGreater(health["latestMillis"], 0)
        for point in result["hotspots"]:
            source = point["sourceId"]
            self.assertIn(source, active)
            product, _, satellite, instrument = feed.SOURCE_PROFILES[source]
            self.assertEqual((product, satellite, instrument), (point["product"], point["satellite"], point["instrument"]))
            self.assertLessEqual(point["time"], result["sourceLatestMillis"][source])
            self.assertGreaterEqual(point["time"], NOW - feed.WINDOW_MILLIS)
            self.assertGreaterEqual(point["frp"], feed.MIN_FRP_MW)
            if instrument == "MODIS":
                self.assertGreaterEqual(point["confidencePercent"], 30)
                self.assertLessEqual(point["confidencePercent"], 100)
                self.assertEqual("n" if point["confidencePercent"] < 80 else "h", point["confidence"])
            else:
                self.assertNotIn("confidencePercent", point)

    def test_shared_synthetic_json_fixtures_match_publisher_contract(self):
        data = inputs()
        examples = {
            "primary_viirs": data,
            "fallback_modis": {"MODIS_NRT": modis_payload(
                modis_row(acq_date="2026-09-05"), modis_row(satellite="A", confidence="79.5"),
            )},
            "partial_viirs": {N20: data[N20]},
        }
        for name, example in examples.items():
            fixture_bytes = (Path(__file__).parent / "fixtures" / (name + ".json")).read_bytes()
            fixture = json.loads(fixture_bytes)
            self.assert_contract(fixture)
            self.assertEqual(feed.build_v2(example, NOW), fixture_bytes)

    def test_primary_prefers_both_viirs_and_preserves_strict_v1(self):
        data = inputs()
        result = document(data)
        self.assert_contract(result)
        self.assertEqual("primary_viirs", result["mode"])
        self.assertEqual(set(feed.SOURCES), set(result["sourceLatestMillis"]))
        self.assertEqual(2, result["candidateCount"])
        old = json.loads(feed.build(all_payloads(), NOW))
        stripped = [{k: v for k, v in p.items() if k not in ("sourceId", "product", "instrument")} for p in result["hotspots"]]
        self.assertEqual(old["hotspots"], stripped)
        with self.assertRaises(ValueError):
            feed.build(data, NOW)  # MODIS can never silently extend Schema 1.
        for bad_modis in (b"<html>bad</html>", modis_payload(), b"\xff"):
            self.assertEqual("primary_viirs", document(all_payloads() | {"MODIS_NRT": bad_modis})["mode"])

    def test_exhaustive_independent_health_and_mode_priority(self):
        # Every combination across the four platforms: 4**4 = 256 scenarios.
        states = ("fresh", "stale", "invalid", "unavailable")
        sources = tuple(feed.SOURCE_PROFILES)
        for combination in itertools.product(states, repeat=4):
            expected = dict(zip(sources, combination))
            data = {}
            modis_rows = []
            for source, state in expected.items():
                product, code, _, instrument = feed.SOURCE_PROFILES[source]
                candidate = modis_row(satellite=code) if instrument == "MODIS" else row(satellite=code)
                if state == "stale":
                    candidate["acq_date"] = "2026-09-05"
                elif state == "invalid":
                    candidate["latitude"] = "nan"
                if instrument == "MODIS":
                    if state != "unavailable":
                        modis_rows.append(candidate)
                elif state != "unavailable":
                    data[product] = payload(candidate)
            data["MODIS_NRT"] = modis_payload(*modis_rows)
            fresh_viirs = [s for s in (N20, N21) if expected[s] == "fresh"]
            fresh_modis = [s for s in (TERRA, AQUA) if expected[s] == "fresh"]
            mode, selected = ("primary_viirs", fresh_viirs) if len(fresh_viirs) == 2 else (
                ("fallback_modis", fresh_modis) if fresh_modis else ("partial_viirs", fresh_viirs)
            )
            with self.subTest(states=combination):
                if not selected:
                    with self.assertRaisesRegex(ValueError, "No fresh approved"):
                        document(data)
                    continue
                result = document(data)
                self.assert_contract(result)
                self.assertEqual(mode, result["mode"])
                self.assertEqual(set(selected), set(result["sourceLatestMillis"]))
                self.assertEqual(expected, {s: h["status"] for s, h in result["sources"].items()})

    def test_modis_transport_failure_marks_both_platforms_unavailable(self):
        error = feed.DownloadError("MODIS_NRT", "HTTP 403")
        result = document(all_payloads(), {"MODIS_NRT": error})
        self.assert_contract(result)
        for source in (TERRA, AQUA):
            self.assertEqual("unavailable", result["sources"][source]["status"])

    def test_health_before_frp_and_confidence_allows_empty_success(self):
        cases = [
            ("primary_viirs", {s: payload(row(satellite=c, frp="0", confidence="l")) for s, (c, _) in feed.SOURCES.items()}),
            ("fallback_modis", {"MODIS_NRT": modis_payload(modis_row(frp="1", confidence="0"))}),
            ("partial_viirs", {N20: payload(row(frp="149.99"))}),
        ]
        for mode, data in cases:
            with self.subTest(mode=mode):
                result = document(data)
                self.assert_contract(result)
                self.assertEqual(mode, result["mode"])
                self.assertEqual([], result["hotspots"])
                self.assertEqual(0, result["candidateCount"])
                self.assertFalse(result["limited"])

    def test_confidence_exact_and_fractional_boundaries_are_own_selection_classes(self):
        for value, classification in ((0, None), (29, None), (29.999, None), (30, "n"), (30.001, "n"), (79, "n"), (79.999, "n"), (80, "h"), (80.001, "h"), (100, "h")):
            with self.subTest(confidence=value):
                result = document({"MODIS_NRT": modis_payload(modis_row(confidence=str(value)))})
                self.assert_contract(result)
                self.assertEqual("fresh", result["sources"][TERRA]["status"])
                if classification is None:
                    self.assertEqual([], result["hotspots"])
                else:
                    self.assertEqual(classification, result["hotspots"][0]["confidence"])
                    self.assertEqual(value, result["hotspots"][0]["confidencePercent"])
        for value in ("", "n", "nan", "inf", "-0.001", "100.001", "101"):
            with self.subTest(invalid=value):
                result = document({N20: payload(row()), "MODIS_NRT": modis_payload(modis_row(confidence=value))})
                self.assertEqual("invalid", result["sources"][TERRA]["status"])
                self.assertEqual("partial_viirs", result["mode"])

    def test_only_collection_61_nrt_establishes_freshness(self):
        for version in ("6.1RT", "6.1URT", "6.0NRT", "6.1", "", "6.1NRT "):
            data = {N20: payload(row()), "MODIS_NRT": modis_payload(modis_row(version=version))}
            result = document(data)
            self.assertEqual("invalid", result["sources"][TERRA]["status"])
            self.assertIsNone(result["sources"][TERRA]["latestMillis"])
        result = document({N20: payload(row()), "MODIS_NRT": modis_payload(
            modis_row(acq_date="2026-09-05"), modis_row(version="6.1RT", acq_time="1200"),
            modis_row(version="6.1URT", acq_time="1200"),
        )})
        self.assertEqual("stale", result["sources"][TERRA]["status"])
        self.assertEqual(NOW - 25 * 3600000, result["sources"][TERRA]["latestMillis"])
        self.assertEqual("partial_viirs", result["mode"])

    def test_invalid_rows_and_wrong_platform_instrument_do_not_poison_valid_peer(self):
        bad = [modis_row(instrument="VIIRS"), modis_row(satellite="N20"), modis_row(satellite="N21"),
               modis_row(latitude="91"), modis_row(longitude="190"), modis_row(frp="-1"),
               modis_row(frp="nan"), modis_row(acq_time="2460"), modis_row(acq_time="1.5")]
        for candidate in bad:
            result = document({"MODIS_NRT": modis_payload(candidate, modis_row(satellite="A"))})
            self.assert_contract(result)
            self.assertEqual({AQUA}, set(result["sourceLatestMillis"]))
        result = document({N20: payload(row(satellite="T", instrument="MODIS")), "MODIS_NRT": modis_payload(modis_row())})
        self.assertEqual("invalid", result["sources"][N20]["status"])

    def test_empty_invalid_schema_encoding_and_input_budget(self):
        cases = [b"<html>bad</html>", b"\xff", modis_payload().replace(b",version", b",unknown"),
                 modis_payload(modis_row()).replace(b"version\r\n", b"instrument\r\n"),
                 modis_payload(modis_row()) + b'"unterminated', b"x" * (feed.MAX_INPUT_BYTES + 1)]
        for content in cases:
            with self.subTest(content=content[:40]):
                result = document({N20: payload(row()), "MODIS_NRT": content})
                self.assertEqual("partial_viirs", result["mode"])
                for source in (TERRA, AQUA):
                    self.assertEqual("invalid", result["sources"][source]["status"])
        for empty in (b"", b" \n\t", b"\xef\xbb\xbf", modis_payload()):
            result = document({N20: payload(row()), "MODIS_NRT": empty})
            for source in (TERRA, AQUA):
                self.assertEqual("unavailable", result["sources"][source]["status"])
        result = document({N20: payload(row()), "MODIS_NRT": modis_payload(modis_row(satellite="unknown"))})
        for source in (TERRA, AQUA):
            self.assertEqual("invalid", result["sources"][source]["status"])

    def test_source_freshness_exact_12_hours_and_no_timestamp_washing(self):
        data = {"MODIS_NRT": modis_payload(modis_row(acq_time="0000"), modis_row(satellite="A", acq_date="2026-09-05", acq_time="2359"))}
        result = document(data)
        self.assert_contract(result)
        self.assertEqual({TERRA}, set(result["sourceLatestMillis"]))
        self.assertEqual("stale", result["sources"][AQUA]["status"])
        with self.assertRaises(ValueError):
            document(data, now=NOW + 1)

    def test_utc_window_future_tolerance_and_raw_latest(self):
        data = {"MODIS_NRT": modis_payload(
            modis_row(acq_date="2026-09-05", acq_time="1200", latitude="1"),
            modis_row(acq_date="2026-09-05", acq_time="1159", latitude="2"),
            modis_row(acq_time="1", latitude="3"),
            modis_row(acq_time="1205", latitude="4", frp="1"),
            modis_row(acq_time="1206", latitude="5"),
        )}
        result = document(data)
        self.assert_contract(result)
        self.assertEqual({1, 3}, {p["lat"] for p in result["hotspots"]})
        self.assertEqual(NOW + feed.FUTURE_MILLIS, result["sourceLatestMillis"][TERRA])
        with self.assertRaises(ValueError):
            document({"MODIS_NRT": modis_payload(modis_row(acq_time="1206"))})

    def test_budget_dedupe_per_platform_dateline_and_deterministic_ties(self):
        rows = [modis_row(longitude=str(18 + i / 10000), frp=str(150 + i)) for i in range(1200)]
        rows += [modis_row(latitude="-40", longitude="-100", satellite="A", confidence="30")]
        rows += [modis_row(longitude="180.2"), modis_row(longitude="-179.8", confidence="100")]
        data = {"MODIS_NRT": modis_payload(*(rows + rows))}
        result = document(data)
        self.assert_contract(result)
        self.assertEqual(1202, result["candidateCount"])
        self.assertEqual(400, len(result["hotspots"]))
        self.assertTrue(result["limited"])
        self.assertLessEqual(len(feed.build_v2(data, NOW)), feed.MAX_OUTPUT_BYTES)
        self.assertEqual(feed.build_v2(data, NOW), feed.build_v2({"MODIS_NRT": modis_payload(*reversed(rows + rows))}, NOW))
        self.assertIn(AQUA, {p["sourceId"] for p in result["hotspots"]})
        # Same pixel/time on different spacecraft remains two observations, never an FRP sum.
        same = document({"MODIS_NRT": modis_payload(modis_row(), modis_row(satellite="A"))})
        self.assertEqual(2, same["candidateCount"])
        self.assertEqual([150, 150], [p["frp"] for p in same["hotspots"]])
        tie = document({"MODIS_NRT": modis_payload(modis_row(confidence="30"), modis_row(confidence="100"))})
        self.assertEqual(100, tie["hotspots"][0]["confidencePercent"])
        with patch.object(feed, "MAX_OUTPUT_BYTES", 100), self.assertRaises(ValueError):
            feed.build_v2(data, NOW)

    def test_closed_schema_evidence_never_prints_upstream_values(self):
        malicious = "https://upstream/" + KEY
        raw = modis_payload(modis_row(instrument=malicious), modis_row(satellite="A"))
        with contextlib.redirect_stdout(io.StringIO()) as output:
            feed.report_input_evidence({"MODIS_NRT": raw}, {N20: ValueError(malicious)}, NOW)
        log = output.getvalue()
        self.assertNotIn(malicious, log)
        self.assertNotIn(KEY, log)
        evidence = json.loads(log.split("NASA MODIS_NRT schema evidence: ")[1])
        self.assertTrue(evidence["instrumentField"])
        self.assertEqual(1, evidence["MODIS"])
        self.assertEqual(1, evidence["otherInstrument"])
        self.assertEqual(2, evidence["collection61NRT"])
        absent = raw.replace(b",instrument", b",unknown")
        with contextlib.redirect_stdout(io.StringIO()) as output:
            feed.report_input_evidence({"MODIS_NRT": absent}, {}, NOW)
        evidence = json.loads(output.getvalue().split("NASA MODIS_NRT schema evidence: ")[1])
        self.assertFalse(evidence["instrumentField"])
        self.assertEqual(2, evidence["missingInstrument"])
        self.assertEqual(0, evidence["MODIS"])
        with contextlib.redirect_stdout(io.StringIO()) as output:
            feed.report_input_evidence({"MODIS_NRT": b"\xff"}, {}, NOW)
        self.assertIn("schema evidence: invalid input", output.getvalue())

    def test_closed_products_and_conflicting_results_fail_closed(self):
        for data, errors in (({"unexpected": b""}, {}), (all_payloads(), {N20: ValueError()}), ({}, {"unknown": ValueError()})):
            with self.assertRaises(ValueError):
                feed.build_v2(data, NOW, errors)
        for now in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                feed.build_v2(inputs(), now)


class PublicationV2Test(unittest.TestCase):
    def test_primary_fallback_partial_recovery_preserves_v1_byte_and_mtime(self):
        with tempfile.TemporaryDirectory() as directory:
            v1, v2 = Path(directory) / "v1.json", Path(directory) / "v2.json"
            result, _, updated = feed.publish_outputs(inputs(), {}, NOW, v1, v2)
            self.assertTrue(updated)
            old, mtime = v1.read_bytes(), v1.stat().st_mtime_ns
            self.assertEqual(feed.build(all_payloads(), NOW), old)
            for data, mode in (({"MODIS_NRT": inputs()["MODIS_NRT"]}, "fallback_modis"), ({N20: inputs()[N20]}, "partial_viirs")):
                result, _, updated = feed.publish_outputs(data, {}, NOW + 60000, v1, v2)
                self.assertFalse(updated)
                self.assertEqual(mode, result["mode"])
                self.assertEqual(old, v1.read_bytes())
                self.assertEqual(mtime, v1.stat().st_mtime_ns)
                self.assertEqual(result, json.loads(v2.read_bytes()))
            result, _, updated = feed.publish_outputs(inputs(), {}, NOW + 120000, v1, v2)
            self.assertTrue(updated)
            self.assertEqual("primary_viirs", result["mode"])
            self.assertEqual(NOW + 120000, json.loads(v1.read_bytes())["generatedAtMillis"])

    def test_total_failure_retains_both_outputs_and_no_temporary_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            v1, v2 = root / "v1.json", root / "v2.json"
            v1.write_bytes(b"previous-v1")
            v2.write_bytes(b"previous-v2")
            before = [p.stat().st_mtime_ns for p in (v1, v2)]
            for data in ({}, {s: b"<html>" for s in feed.PRODUCTS}, {"MODIS_NRT": modis_payload(modis_row(acq_date="2026-09-04"))}):
                with self.assertRaises(ValueError):
                    feed.publish_outputs(data, {}, NOW, v1, v2)
                self.assertEqual(b"previous-v1", v1.read_bytes())
                self.assertEqual(b"previous-v2", v2.read_bytes())
                self.assertEqual(before, [p.stat().st_mtime_ns for p in (v1, v2)])
                self.assertEqual([], list(root.glob("*.tmp")))

    def test_atomic_replace_and_cleanup_if_fsync_or_replace_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "v2.json"
            path.write_bytes(b"previous")
            for target in ("build_feed.os.fsync", "build_feed.Path.replace"):
                with patch(target, side_effect=OSError("synthetic write error")), self.assertRaises(OSError):
                    feed.write_feed(path, b"new")
                self.assertEqual(b"previous", path.read_bytes())
                self.assertEqual([], list(path.parent.glob("*.tmp")))
            real_replace = Path.replace
            observed = []

            def replace(temporary, destination):
                observed.append((temporary.parent == path.parent, temporary.read_bytes(), path.read_bytes()))
                return real_replace(temporary, destination)

            with patch.object(Path, "replace", new=replace):
                feed.write_feed(path, b"complete new document")
            self.assertEqual([(True, b"complete new document", b"previous")], observed)
            self.assertEqual(b"complete new document", path.read_bytes())

    def test_same_output_path_rejected_before_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "feed.json"
            with self.assertRaises(ValueError):
                feed.publish_outputs(inputs(), {}, NOW, path, path)
            self.assertFalse(path.exists())

    def test_real_cli_subprocess_offline_bootstrap_and_total_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            when = datetime.now(timezone.utc)
            raw = modis_row(acq_date=when.strftime("%Y-%m-%d"), acq_time=when.strftime("%H%M"))
            csv_path = root / "MODIS_NRT.csv"
            csv_path.write_bytes(modis_payload(raw))
            v1, v2 = root / "v1.json", root / "v2.json"
            command = [sys.executable, str(Path(feed.__file__).resolve()), "--input-dir", str(root), "--output", str(v1), "--output-v2", str(v2)]
            env = {"PATH": os.defpath, "TMPDIR": tempfile.gettempdir()}
            result = subprocess.run(command, cwd=root, env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual("fallback_modis", json.loads(v2.read_bytes())["mode"])
            self.assertFalse(v1.exists())
            previous, mtime = v2.read_bytes(), v2.stat().st_mtime_ns
            csv_path.unlink()
            result = subprocess.run(command, cwd=root, env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(1, result.returncode)
            self.assertIn("No fresh approved NASA source", result.stderr)
            self.assertEqual(previous, v2.read_bytes())
            self.assertEqual(mtime, v2.stat().st_mtime_ns)
            self.assertFalse(v1.exists())

    def test_cli_offline_missing_products_fallback_and_safe_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "MODIS_NRT.csv").write_bytes(modis_payload(modis_row()))
            v1, v2, summary = root / "v1.json", root / "v2.json", root / "summary.md"
            v1.write_bytes(b"old v1")
            args = ["build_feed.py", "--input-dir", str(root), "--output", str(v1), "--output-v2", str(v2)]
            with patch("sys.argv", args), patch.object(feed.time, "time", return_value=NOW / 1000), patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": str(summary)}), patch.object(feed, "download_once", side_effect=AssertionError("offline test attempted network")), contextlib.redirect_stdout(io.StringIO()) as log:
                self.assertEqual(0, feed.main())
            self.assertEqual(b"old v1", v1.read_bytes())
            self.assertEqual("fallback_modis", json.loads(v2.read_bytes())["mode"])
            self.assertIn("VIIRS primary pair degraded", log.getvalue())
            self.assertIn("V1: unchanged", summary.read_text())
            self.assertIn("MODIS_TERRA_NRT: fresh", summary.read_text())
            self.assertNotIn(KEY, log.getvalue() + summary.read_text())

    def test_cli_with_collected_partial_error_succeeds_without_logging_untrusted_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            v1, v2 = root / "v1.json", root / "v2.json"
            errors = {N21: ValueError("https://upstream/" + KEY)}
            with patch("sys.argv", ["build_feed.py", "--output", str(v1), "--output-v2", str(v2)]), patch.dict(os.environ, {"FIRMS_MAP_KEY": KEY, "GITHUB_STEP_SUMMARY": ""}), patch.object(feed.time, "time", return_value=NOW / 1000), patch.object(feed, "download_results", return_value=({"MODIS_NRT": modis_payload(modis_row())}, errors)), contextlib.redirect_stdout(io.StringIO()) as log:
                self.assertEqual(0, feed.main())
            self.assertFalse(v1.exists())
            self.assertTrue(v2.exists())
            self.assertNotIn(KEY, log.getvalue())


class TransportV2Test(unittest.TestCase):
    def test_all_three_products_are_parallel_in_both_retry_rounds(self):
        barrier = threading.Barrier(3, timeout=3)
        calls = []

        def fail(_key, product):
            calls.append(product)
            barrier.wait()
            raise feed.DownloadError(product, "transport code 28", True)

        with patch.object(feed, "download_once", side_effect=fail), patch.object(feed.time, "sleep") as slept:
            data, errors = feed.download_results(KEY)
        self.assertEqual({}, data)
        self.assertEqual(set(feed.PRODUCTS), set(errors))
        for product in feed.PRODUCTS:
            self.assertEqual(6, calls.count(product))
            self.assertTrue(errors[product].retryable)
        expected = [2, 4] * 6 + [feed.SOURCE_RETRY_DELAY_SECONDS]
        self.assertEqual(sorted(expected), sorted(call.args[0] for call in slept.call_args_list))
        self.assertEqual(342, 2 * (3 * feed.PROCESS_TIMEOUT_SECONDS + 2 + 4) + feed.SOURCE_RETRY_DELAY_SECONDS)

    def test_permanent_failure_does_not_cancel_other_transient_recovery_or_success(self):
        calls = []

        def fetch(_key, product):
            calls.append(product)
            if product == N20:
                raise feed.DownloadError(product, "HTTP 403")
            if product == N21 and calls.count(product) == 1:
                raise feed.DownloadError(product, "transport code 56", True)
            return inputs()[product]

        with patch.object(feed, "download", side_effect=fetch), patch.object(feed.time, "sleep") as slept:
            data, errors = feed.download_results(KEY)
        self.assertEqual({N21, "MODIS_NRT"}, set(data))
        self.assertEqual({N20}, set(errors))
        self.assertEqual((1, 2, 1), tuple(calls.count(p) for p in feed.PRODUCTS))
        slept.assert_called_once_with(feed.SOURCE_RETRY_DELAY_SECONDS)
        self.assertEqual("fallback_modis", document(data, errors)["mode"])

    def test_permanent_modis_failure_preserves_viirs_success_without_retry(self):
        def fetch(_key, product):
            if product == "MODIS_NRT":
                raise feed.DownloadError(product, "transport code 60")
            return inputs()[product]

        with patch.object(feed, "download", side_effect=fetch) as request, patch.object(feed.time, "sleep") as slept:
            data, errors = feed.download_results(KEY)
        self.assertEqual(3, request.call_count)
        slept.assert_not_called()
        self.assertEqual("primary_viirs", document(data, errors)["mode"])

    def test_successes_survive_exhausted_temporary_errors(self):
        def fetch(_key, product):
            if product == "MODIS_NRT":
                return inputs()[product]
            raise feed.DownloadError(product, "transport code 28", True)

        with patch.object(feed, "download", side_effect=fetch) as request, patch.object(feed.time, "sleep"):
            data, errors = feed.download_results(KEY)
        self.assertEqual(5, request.call_count)
        self.assertEqual({"MODIS_NRT"}, set(data))
        self.assertEqual({N20, N21}, set(errors))
        self.assertEqual("fallback_modis", document(data, errors)["mode"])

    def test_closed_parallelism_and_permanent_diagnostics(self):
        with patch.object(feed, "download") as request:
            for products in ((), ("MODIS_NRT", "MODIS_NRT"), ("unexpected",)):
                with self.assertRaises(ValueError):
                    feed.download_results(KEY, products)
            request.assert_not_called()
        self.assertEqual("NASA HTTP 403 for MODIS_NRT", feed.safe_failure_reason(feed.DownloadError("MODIS_NRT", "HTTP 403")))
        self.assertNotIn(KEY, feed.safe_failure_reason(ValueError(KEY)))


class WorkflowV2Test(unittest.TestCase):
    def test_both_future_publish_jobs_stage_and_read_back_both_files(self):
        # stdlib-only check: no Actions dispatch, credentials or Git remote writes.
        workflow = (Path(__file__).parent / ".github/workflows/update-fire-feed.yml").read_text()
        command = "python3 build_feed.py --output data/fire-hotspots.json --output-v2 data/fire-hotspots-v2.json"
        self.assertEqual(2, workflow.count(command))
        self.assertEqual(4, workflow.count("for path in data/fire-hotspots.json data/fire-hotspots-v2.json; do"))
        self.assertEqual(2, workflow.count('test "$(git rev-parse "HEAD:$path")" = "$(git rev-parse "origin/main:$path")"'))
        self.assertEqual(2, workflow.count("if: steps.build.outcome == 'success'"))
        self.assertIn("timeout-minutes: 12", workflow)
        self.assertEqual(2, workflow.count("timeout-minutes: 7"))
        self.assertNotIn("--force", workflow)
        scripts = []
        current = None
        for line in workflow.splitlines():
            if line == "        run: |":
                current = []
                scripts.append(current)
            elif current is not None and line.startswith("          "):
                current.append(line[10:])
            elif line.strip():
                current = None
        self.assertEqual(3, len(scripts))
        for script in scripts:
            result = subprocess.run(["bash", "-n"], input="\n".join(script), text=True, capture_output=True, timeout=5)
            self.assertEqual(0, result.returncode, result.stderr)


if __name__ == "__main__":
    unittest.main()
