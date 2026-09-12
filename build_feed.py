#!/usr/bin/env python3
"""Generate the small public world feed; NASA credentials and raw CSV stay server-side."""

import argparse
import csv
import io
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

MIN_FRP_MW = 150.0
MAX_HOTSPOTS = 400
MAX_OUTPUT_BYTES = 256 * 1024
MAX_INPUT_BYTES = 20 * 1024 * 1024  # per satellite, on the job host only
WINDOW_MILLIS = 24 * 60 * 60 * 1000
FUTURE_MILLIS = 5 * 60 * 1000
SOURCE_MAX_AGE_MILLIS = 12 * 60 * 60 * 1000
MAX_DOWNLOAD_ATTEMPTS = 3
CONNECT_TIMEOUT_SECONDS = 10
DOWNLOAD_TIMEOUT_SECONDS = 45
PROCESS_TIMEOUT_SECONDS = 50
SOURCE_RETRY_DELAY_SECONDS = 30
NASA_BASE_URL = "https://firms.modaps.eosdis.nasa.gov"
TRANSIENT_HTTP_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}
# DNS/connect, partial transfers, timeout, empty reply, send/receive, HTTP/2/3.
# TLS certificate errors, redirects, credentials and size limits are NOT transient.
TRANSIENT_CURL_CODES = {5, 6, 7, 16, 18, 28, 52, 55, 56, 92, 95, 96}
SOURCES = {
    "VIIRS_NOAA20_NRT": ("N20", "NOAA-20 VIIRS"),
    "VIIRS_NOAA21_NRT": ("N21", "NOAA-21 VIIRS"),
}
FIELDS = {"latitude", "longitude", "acq_date", "acq_time", "frp", "confidence", "satellite", "instrument"}
CONFIDENCE_ALIASES = {"l": "l", "n": "n", "h": "h", "low": "l", "nominal": "n", "high": "h"}


def normalize_coordinates(latitude, longitude):
    # NRT geolocation can overshoot the dateline or poles by a fraction of a degree.
    if abs(latitude) <= 90.01:
        latitude = max(-90.0, min(90.0, latitude))
    else:
        return None
    if abs(longitude) <= 181.0:
        longitude = (longitude + 180.0) % 360.0 - 180.0
        if longitude <= -180.0:
            longitude = 180.0
    else:
        return None
    return latitude, longitude


def parse_row(row, satellite_code, satellite_name, now):
    try:
        latitude, longitude, power = (float(row[field]) for field in ("latitude", "longitude", "frp"))
    except (KeyError, TypeError, ValueError):
        return None
    if not all(math.isfinite(value) for value in (latitude, longitude, power)) or power < 0:
        return None
    coordinates = normalize_coordinates(latitude, longitude)
    if coordinates is None:
        return None
    latitude, longitude = coordinates
    if row.get("satellite") != satellite_code or row.get("instrument") != "VIIRS":
        return None
    confidence = CONFIDENCE_ALIASES.get(str(row.get("confidence", "")).strip().lower())
    if confidence is None:
        return None
    clock = str(row.get("acq_time", "")).strip()
    if not re.fullmatch(r"\d{1,4}", clock):
        return None
    try:
        when = datetime.strptime(str(row.get("acq_date", "")) + clock.zfill(4), "%Y-%m-%d%H%M").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    measured = int(when.timestamp() * 1000)
    if measured > now + FUTURE_MILLIS:
        return None
    record = None
    if not (measured < now - WINDOW_MILLIS or power < MIN_FRP_MW or confidence == "l"):
        record = {
            "lat": latitude,
            "lon": longitude,
            "time": measured,
            "frp": power,
            "confidence": confidence,
            "satellite": satellite_name,
        }
    return measured, record


def parse_csv(payload, source, now):
    if len(payload) > MAX_INPUT_BYTES:
        raise ValueError("NASA response exceeds input budget")
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig")))
    if not FIELDS.issubset(reader.fieldnames or []):
        raise ValueError("NASA CSV schema missing")
    satellite_code, satellite_name = SOURCES[source]
    latest = 0
    records = []
    for row in reader:
        # Skip individual unusable NRT rows. Fail the refresh only when the source as
        # a whole is missing, empty, or older than SOURCE_MAX_AGE_MILLIS.
        parsed = parse_row(row, satellite_code, satellite_name, now)
        if parsed is None:
            continue
        measured, record = parsed
        latest = max(latest, measured)
        if record is not None:
            records.append(record)
    if latest < now - SOURCE_MAX_AGE_MILLIS:
        raise ValueError("NASA satellite feed is empty or outdated")
    return records, latest


def identity(record):
    return (record["satellite"], record["time"], round(record["lat"], 5), round(record["lon"], 5))


def priority(record):
    return (-record["frp"], -record["time"], identity(record))


def select(records):
    unique = {}
    for record in sorted(records, key=priority):
        unique.setdefault(identity(record), record)
    # Round-robin across 30-degree geographic cells, strongest per cell first.
    # Every occupied cell gets a turn before another detection is taken from a dense cell.
    cells = defaultdict(deque)
    for record in unique.values():
        cell = (min(5, int((record["lat"] + 90) // 30)), min(11, int((record["lon"] + 180) // 30)))
        cells[cell].append(record)
    selected = []
    while cells and len(selected) < MAX_HOTSPOTS:
        keys = sorted(cells, key=lambda cell: priority(cells[cell][0]))
        for cell in keys:
            selected.append(cells[cell].popleft())
            if not cells[cell]:
                del cells[cell]
            if len(selected) == MAX_HOTSPOTS:
                break
    return sorted(selected, key=priority), len(unique)


def build(payloads, now):
    if set(payloads) != set(SOURCES):
        raise ValueError("Both global satellite feeds are required")
    records, latest = [], {}
    for source in SOURCES:
        points, latest[source] = parse_csv(payloads[source], source, now)
        records.extend(points)
    selected, candidates = select(records)
    document = {
        "schemaVersion": 1,
        "coverage": "world",
        "generatedAtMillis": now,
        "minFrpMw": MIN_FRP_MW,
        "windowHours": 24,
        "sourceLatestMillis": latest,
        "candidateCount": candidates,
        "limited": candidates > len(selected),
        "hotspots": selected,
    }
    content = (json.dumps(document, separators=(",", ":"), allow_nan=False) + "\n").encode()
    if len(content) > MAX_OUTPUT_BYTES:
        raise ValueError("Filtered feed exceeds mobile budget")
    return content


class DownloadError(ValueError):
    def __init__(self, source, category, retryable=False):
        super().__init__(f"NASA {category} for {source}")
        self.retryable = retryable


def download_once(key, source):
    # urllib's socket timeout is NOT a wall-clock transfer deadline. A trickling
    # response or repeated connection attempts used to consume the entire CI job.
    # curl bounds connection AND total transfer; the subprocess is a final guard.
    if source not in SOURCES or not re.fullmatch(r"[a-fA-F0-9]{32}", key):
        raise ValueError("FIRMS_MAP_KEY is missing or invalid")
    url = f"{NASA_BASE_URL}/api/area/csv/{key}/{source}/world/2"
    with tempfile.TemporaryDirectory(prefix="planetarylens-fire-") as directory:
        output = Path(directory) / "response.csv"
        command = [
            "curl",
            "--disable",
            "--silent",
            "--fail",
            "--proto",
            "=https",
            "--connect-timeout",
            str(CONNECT_TIMEOUT_SECONDS),
            "--max-time",
            str(DOWNLOAD_TIMEOUT_SECONDS),
            "--max-filesize",
            str(MAX_INPUT_BYTES),
            "--header",
            "Accept: text/csv",
            "--user-agent",
            "PlanetaryLens-FireFeed/1.0",
            "--output",
            str(output),
            "--write-out",
            "%{http_code}",
            "--config",
            "-",
        ]
        try:
            # The credential-bearing URL is passed over stdin, never argv or logs.
            # No --location: a redirect must never forward the NASA credential.
            result = subprocess.run(
                command,
                input=f'url = "{url}"\n',
                capture_output=True,
                text=True,
                timeout=PROCESS_TIMEOUT_SECONDS,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise DownloadError(source, "process deadline exceeded", True) from None
        except OSError:
            raise DownloadError(source, "transport unavailable") from None
        status_text = result.stdout.strip()
        status = int(status_text) if re.fullmatch(r"[0-9]{3}", status_text) else 0
        if status >= 300:
            raise DownloadError(source, f"HTTP {status}", status in TRANSIENT_HTTP_STATUS)
        if result.returncode:
            raise DownloadError(
                source, f"transport code {result.returncode}", result.returncode in TRANSIENT_CURL_CODES
            )
        if status != 200:
            raise DownloadError(source, "unexpected HTTP response")
        with output.open("rb") as response:
            content = response.read(MAX_INPUT_BYTES + 1)
        if len(content) > MAX_INPUT_BYTES:
            raise ValueError("NASA response exceeds input budget")
        return content


def download(key, source):
    for attempt in range(MAX_DOWNLOAD_ATTEMPTS):
        started = time.monotonic()
        try:
            content = download_once(key, source)
            print(
                f"NASA {source}: received {len(content)} bytes in {time.monotonic() - started:.1f}s",
                flush=True,
            )
            return content
        except DownloadError as error:
            print(
                f"Attempt {attempt + 1}/{MAX_DOWNLOAD_ATTEMPTS}: {safe_failure_reason(error)} "
                f"({time.monotonic() - started:.1f}s)",
                file=sys.stderr,
                flush=True,
            )
            if not error.retryable or attempt == MAX_DOWNLOAD_ATTEMPTS - 1:
                raise
            time.sleep(2 ** (attempt + 1))


def download_all(key):
    payloads, errors = {}, {}
    pending = list(SOURCES)
    for round_index in range(2):
        if round_index:
            print("NASA temporary failure: retrying only missing satellites after cooldown", flush=True)
            time.sleep(SOURCE_RETRY_DELAY_SECONDS)
        # Both rounds MUST be parallel. Serial retries could exceed the
        # 12-minute job budget when both satellites failed.
        with ThreadPoolExecutor(max_workers=len(SOURCES)) as pool:
            futures = {source: pool.submit(download, key, source) for source in pending}
            for source, future in futures.items():
                try:
                    payloads[source] = future.result()
                    errors.pop(source, None)
                except Exception as error:
                    errors[source] = error
        if not errors:
            return payloads
        # Never retry permanent failures in the outer round (including HTTP 403).
        permanent = [
            error for error in errors.values() if not isinstance(error, DownloadError) or not error.retryable
        ]
        if permanent:
            raise permanent[0]
        pending = list(errors)
    raise next(iter(errors.values()))


def safe_failure_reason(error):
    # Never print arbitrary upstream exceptions: float/date parsing and network errors may
    # contain untrusted input or a credential-bearing URL. Only our fixed diagnostics qualify.
    allowed = {
        "NASA response exceeds input budget",
        "NASA CSV schema missing",
        "NASA satellite feed is empty or outdated",
        "Both global satellite feeds are required",
        "Filtered feed exceeds mobile budget",
        "FIRMS_MAP_KEY is missing or invalid",
    } | {
        f"NASA {category} for {source}"
        for source in SOURCES
        for category in (
            "process deadline exceeded",
            "transport unavailable",
            "unexpected HTTP response",
            *(f"HTTP {status}" for status in range(300, 600)),
            *(f"transport code {code}" for code in range(1, 100)),
        )
    }
    reason = str(error)
    return reason if reason in allowed else "Unexpected input or processing error"


def write_feed(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(content)
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/fire-hotspots.json"))
    parser.add_argument("--input-dir", type=Path, help="Local NASA CSV directory for offline verification")
    args = parser.parse_args()
    try:
        if args.input_dir:
            payloads = {source: (args.input_dir / (source + ".csv")).read_bytes() for source in SOURCES}
        else:
            key = os.environ.get("FIRMS_MAP_KEY", "").strip()
            if not re.fullmatch(r"[a-fA-F0-9]{32}", key):
                raise ValueError("FIRMS_MAP_KEY is missing or invalid")
            payloads = download_all(key)
        content = build(payloads, int(time.time() * 1000))
        write_feed(args.output, content)
        document = json.loads(content)
        print(
            f"Built selection: {len(document['hotspots'])} / {document['candidateCount']} detections, {len(content)} bytes"
        )
    except Exception as error:
        # No raw upstream payload, secret, URL, or traceback in public workflow logs.
        print(
            f"Feed refresh failed: {safe_failure_reason(error)}; previous output retained.", file=sys.stderr
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
