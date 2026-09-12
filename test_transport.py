"""Offline retry tests plus real curl/TLS regression tests against loopback only."""

import contextlib
import io
import os
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import cast
from unittest.mock import patch

import build_feed as feed
from test_build_feed import NOW, all_payloads

KEY = "a" * 32  # Synthetic fixture, not a credential.
SOURCE = "VIIRS_NOAA20_NRT"


class RetryTest(unittest.TestCase):
    def test_transient_failure_recovers_with_bounded_backoff(self):
        error = feed.DownloadError(SOURCE, "HTTP 503", True)
        with (
            patch.object(feed, "download_once", side_effect=[error, b"fixture"]) as request,
            patch.object(feed.time, "sleep") as sleep,
        ):
            self.assertEqual(b"fixture", feed.download(KEY, SOURCE))
        self.assertEqual(2, request.call_count)
        sleep.assert_called_once_with(2)

    def test_permanent_failure_does_not_retry_in_either_round(self):
        for category in (
            "HTTP 403",
            "HTTP 302",
            "transport code 60",
            "transport code 63",
            "transport unavailable",
        ):

            def fake_request(_key, source, category=category):
                if source == SOURCE:
                    raise feed.DownloadError(source, category)
                return b"fixture"

            with (
                self.subTest(category=category),
                patch.object(feed, "download_once", side_effect=fake_request) as request,
                patch.object(feed.time, "sleep") as sleep,
            ):
                with self.assertRaises(feed.DownloadError):
                    feed.download_all(KEY)
                self.assertEqual(2, request.call_count)
                sleep.assert_not_called()

    def test_both_retry_rounds_run_in_parallel_and_exhaust_within_budget(self):
        barrier = threading.Barrier(2, timeout=3)
        calls = []

        def failure(_key, source):
            calls.append(source)
            barrier.wait()  # A serial recovery round fails this test.
            raise feed.DownloadError(source, "transport code 28", True)

        with (
            patch.object(feed, "download_once", side_effect=failure),
            patch.object(feed.time, "sleep") as sleep,
        ):
            with self.assertRaises(feed.DownloadError):
                feed.download_all(KEY)
        for source in feed.SOURCES:
            self.assertEqual(2 * feed.MAX_DOWNLOAD_ATTEMPTS, calls.count(source))
        expected_delays = [2, 4] * 4 + [feed.SOURCE_RETRY_DELAY_SECONDS]
        self.assertEqual(sorted(expected_delays), sorted(call.args[0] for call in sleep.call_args_list))
        budget = (
            2 * (feed.MAX_DOWNLOAD_ATTEMPTS * feed.PROCESS_TIMEOUT_SECONDS + 2 + 4)
            + feed.SOURCE_RETRY_DELAY_SECONDS
        )
        self.assertEqual(342, budget)
        self.assertLess(budget, 6 * 60)

    def test_argv_and_diagnostics_never_contain_key_url_or_raw_errors(self):
        for failure in (subprocess.TimeoutExpired([KEY], 50, output=KEY, stderr=KEY), OSError(KEY)):
            with patch.object(feed.subprocess, "run", side_effect=failure) as run:
                with self.assertRaises(feed.DownloadError) as caught:
                    feed.download_once(KEY, SOURCE)
            args, kwargs = run.call_args
            self.assertNotIn(KEY, repr(args))
            self.assertNotIn(feed.NASA_BASE_URL, repr(args))
            self.assertIn(KEY, kwargs["input"])
            self.assertEqual(feed.PROCESS_TIMEOUT_SECONDS, kwargs["timeout"])
            self.assertNotIn(KEY, str(caught.exception))
            self.assertNotIn(KEY, feed.safe_failure_reason(caught.exception))

    def test_rejects_config_injection_before_starting_transport(self):
        with patch.object(feed.subprocess, "run") as run:
            for key, source in ((KEY + '\nurl="https://example.org"', SOURCE), (KEY, SOURCE + '"')):
                with self.assertRaises(ValueError):
                    feed.download_once(key, source)
            run.assert_not_called()

    def test_cli_retains_previous_output_on_transport_and_validation_failure(self):
        invalid = all_payloads() | {SOURCE: b"<html>upstream unavailable</html>"}
        stale = all_payloads()
        stale[SOURCE] = stale[SOURCE].replace(b"2026-09-06", b"2026-09-04")
        for response in (feed.DownloadError(SOURCE, "transport code 28", True), invalid, stale):
            with self.subTest(response=type(response).__name__), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "feed.json"
                output.write_bytes(b"previous output")
                download = (
                    patch.object(feed, "download_all", side_effect=response)
                    if isinstance(response, Exception)
                    else patch.object(feed, "download_all", return_value=response)
                )
                with (
                    download,
                    patch.dict(os.environ, {"FIRMS_MAP_KEY": KEY}),
                    patch.object(feed.time, "time", return_value=NOW / 1000),
                    patch("sys.argv", ["build_feed.py", "--output", str(output)]),
                    contextlib.redirect_stderr(io.StringIO()) as errors,
                ):
                    self.assertEqual(1, feed.main())
                self.assertEqual(b"previous output", output.read_bytes())
                self.assertNotIn(KEY, errors.getvalue())
                self.assertFalse(output.with_suffix(".tmp").exists())


class TestServer(ThreadingHTTPServer):
    mode: str | int = "ok"
    requests = 0


class LoopbackTransportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="fire-transport-test-")
        cls.addClassCleanup(cls.directory.cleanup)
        root = Path(cls.directory.name)
        cert, key = root / "cert.pem", root / "key.pem"
        # Generated ephemeral loopback certificate; no checked-in private keys.
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-keyout",
                str(key),
                "-out",
                str(cert),
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost",
            ],
            check=True,
            capture_output=True,
            timeout=10,
        )
        cls.cert = str(cert)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass  # Never log paths (even though all keys are synthetic).

            def do_GET(self):
                server = cast(TestServer, self.server)
                server.requests += 1
                mode = server.mode
                code = mode if isinstance(mode, int) else 200
                self.send_response(code)
                if code == 302:
                    self.send_header("Location", "/must-not-follow")
                if mode in ("oversize", "truncated"):
                    self.send_header("Content-Length", "2048")
                self.end_headers()
                try:
                    if mode == "slow":
                        # Frequent bytes defeat a socket inactivity timeout, not max-time.
                        for _ in range(40):
                            self.wfile.write(b"x")
                            self.wfile.flush()
                            time.sleep(0.1)
                    elif mode == "chunked-oversize":
                        self.wfile.write(b"x" * 2048)
                    else:
                        self.wfile.write(b"csv fixture")
                except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
                    pass

        cls.server = TestServer(("127.0.0.1", 0), Handler)
        cls.server.socket = context.wrap_socket(cls.server.socket, server_side=True)
        cls.server.mode = "ok"
        cls.server.requests = 0
        cls.addClassCleanup(cls.server.server_close)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.addClassCleanup(cls.server.shutdown)

    def setUp(self):
        self.server.requests = 0
        self.server.mode = "ok"
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(
            patch.object(feed, "NASA_BASE_URL", f"https://localhost:{self.server.server_port}")
        )
        self.stack.enter_context(
            patch.dict(
                os.environ, {"CURL_CA_BUNDLE": self.cert, "NO_PROXY": "localhost", "no_proxy": "localhost"}
            )
        )

    def test_real_https_download(self):
        self.assertEqual(b"csv fixture", feed.download_once(KEY, SOURCE))
        self.assertEqual(1, self.server.requests)

    def test_slow_drip_is_stopped_by_total_transfer_deadline(self):
        self.server.mode = "slow"
        started = time.monotonic()
        with (
            patch.object(feed, "DOWNLOAD_TIMEOUT_SECONDS", 1),
            self.assertRaises(feed.DownloadError) as caught,
        ):
            feed.download_once(KEY, SOURCE)
        elapsed = time.monotonic() - started
        self.assertIn("transport code 28", str(caught.exception))
        self.assertTrue(caught.exception.retryable)
        self.assertLess(elapsed, 3)
        self.assertEqual(1, self.server.requests)

    def test_process_guard_kills_transfer_even_if_curl_deadline_is_long(self):
        self.server.mode = "slow"
        started = time.monotonic()
        with (
            patch.object(feed, "DOWNLOAD_TIMEOUT_SECONDS", 30),
            patch.object(feed, "PROCESS_TIMEOUT_SECONDS", 1),
            self.assertRaises(feed.DownloadError) as caught,
        ):
            feed.download_once(KEY, SOURCE)
        self.assertIn("process deadline exceeded", str(caught.exception))
        self.assertTrue(caught.exception.retryable)
        self.assertLess(time.monotonic() - started, 3)

    def test_http_status_is_classified_and_redirects_are_not_followed(self):
        for status, transient in (
            (302, False),
            (401, False),
            (403, False),
            (429, True),
            (500, True),
            (503, True),
        ):
            with self.subTest(status=status):
                self.server.mode = status
                self.server.requests = 0
                with self.assertRaises(feed.DownloadError) as caught:
                    feed.download_once(KEY, SOURCE)
                self.assertEqual(
                    f"NASA HTTP {status} for {SOURCE}", feed.safe_failure_reason(caught.exception)
                )
                self.assertEqual(transient, caught.exception.retryable)
                self.assertEqual(1, self.server.requests)

    def test_announced_and_unannounced_oversize_bodies_are_rejected(self):
        for mode in ("oversize", "chunked-oversize"):
            with self.subTest(mode=mode), patch.object(feed, "MAX_INPUT_BYTES", 32):
                self.server.mode = mode
                with self.assertRaises(ValueError):
                    feed.download_once(KEY, SOURCE)

    def test_truncated_body_is_retryable_not_accepted_as_a_partial_feed(self):
        self.server.mode = "truncated"
        with self.assertRaises(feed.DownloadError) as caught:
            feed.download_once(KEY, SOURCE)
        self.assertIn("transport code 18", str(caught.exception))
        self.assertTrue(caught.exception.retryable)

    def test_untrusted_tls_certificate_is_not_bypassed_or_retried(self):
        # Remove only the test CA override. Production always validates TLS.
        with patch.dict(os.environ):
            os.environ.pop("CURL_CA_BUNDLE", None)
            with self.assertRaises(feed.DownloadError) as caught:
                feed.download_once(KEY, SOURCE)
        self.assertIn("transport code 60", str(caught.exception))
        self.assertFalse(caught.exception.retryable)


if __name__ == "__main__":
    unittest.main()
