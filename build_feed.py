#!/usr/bin/env python3
"""Generate the small public world feed; NASA credentials and raw CSV stay server-side."""

import argparse
import csv
import http.client
import io
import json
import math
import os
import re
import socket
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

MIN_FRP_MW = 50.0
MAX_HOTSPOTS = 400
MAX_OUTPUT_BYTES = 256 * 1024
MAX_INPUT_BYTES = 20 * 1024 * 1024  # per satellite, on the job host only
WINDOW_MILLIS = 24 * 60 * 60 * 1000
FUTURE_MILLIS = 5 * 60 * 1000
SOURCE_MAX_AGE_MILLIS = 12 * 60 * 60 * 1000
MAX_DOWNLOAD_ATTEMPTS = 5
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
        when = datetime.strptime(
            str(row.get("acq_date", "")) + clock.zfill(4), "%Y-%m-%d%H%M"
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    measured = int(when.timestamp() * 1000)
    if measured > now + FUTURE_MILLIS:
        return None
    record = None
    if not (measured < now - WINDOW_MILLIS or power < MIN_FRP_MW or confidence == "l"):
        record = {
            "lat": latitude, "lon": longitude, "time": measured, "frp": power,
            "confidence": confidence, "satellite": satellite_name,
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


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("NASA redirect refused")


def is_transient(error):
    if isinstance(error, urllib.error.HTTPError):
        return error.code in (408, 409, 425, 429, 500, 502, 503, 504)
    if isinstance(error, urllib.error.URLError):
        return True
    return isinstance(error, (TimeoutError, socket.timeout, ConnectionError, http.client.IncompleteRead, OSError))


def download(key, source):
    # Two UTC calendar days cover the rolling 24-hour interval; filter exact times in build().
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{key}/{source}/world/2"
    request = urllib.request.Request(url, headers={"Accept": "text/csv", "User-Agent": "PlanetaryLens-FireFeed/1.0"})
    for attempt in range(MAX_DOWNLOAD_ATTEMPTS):
        try:
            with urllib.request.build_opener(NoRedirect).open(request, timeout=45) as response:
                content = response.read(MAX_INPUT_BYTES + 1)
            if len(content) > MAX_INPUT_BYTES:
                raise ValueError("NASA response exceeds input budget")
            return content
        except Exception as error:
            if is_transient(error) and attempt < MAX_DOWNLOAD_ATTEMPTS - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            # Request URLs can contain the key. Forward only a fixed, known message.
            if isinstance(error, TimeoutError) or (
                isinstance(error, urllib.error.URLError) and isinstance(error.reason, TimeoutError)
            ):
                raise ValueError(f"NASA download timed out for {source}") from None
            if isinstance(error, urllib.error.HTTPError):
                raise ValueError(f"NASA HTTP request failed for {source}") from None
            if isinstance(error, urllib.error.URLError):
                raise ValueError(f"NASA connection failed for {source}") from None
            raise ValueError(f"NASA download failed for {source}") from None


def download_all(key):
    payloads = {}
    errors = []
    with ThreadPoolExecutor(max_workers=len(SOURCES)) as pool:
        futures = {source: pool.submit(download, key, source) for source in SOURCES}
        for source, future in futures.items():
            try:
                payloads[source] = future.result()
            except Exception as error:
                errors.append(error)
    if errors:
        raise errors[0]
    return payloads


def safe_failure_reason(error):
    # Never print arbitrary upstream exceptions: float/date parsing and network errors may
    # contain untrusted input or a credential-bearing URL. Only our fixed diagnostics qualify.
    allowed = {
        "NASA response exceeds input budget", "NASA CSV schema missing", "Non-finite NASA measurement",
        "Invalid NASA measurement", "Unexpected NASA sensor", "Unexpected NASA confidence",
        "Invalid NASA acquisition time", "NASA measurement is in the future",
        "NASA satellite feed is empty or outdated", "Both global satellite feeds are required",
        "Filtered feed exceeds mobile budget", "FIRMS_MAP_KEY is missing or invalid",
    } | {
        f"NASA {reason} for {source}"
        for source in SOURCES
        for reason in ("download failed", "download timed out", "HTTP request failed", "connection failed")
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
        print(f"Published selection: {len(document['hotspots'])} / {document['candidateCount']} detections, {len(content)} bytes")
    except Exception as error:
        # No raw upstream payload, secret, URL, or traceback in public workflow logs.
        print(f"Feed refresh failed: {safe_failure_reason(error)}; previous output retained.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
