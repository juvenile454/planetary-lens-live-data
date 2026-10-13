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
CONNECT_TIMEOUT_SECONDS = 20
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
# SOURCES and build() retain the strict two-source V1 contract.
PRODUCTS = (*SOURCES, "MODIS_NRT")
SOURCE_PROFILES = {
    "VIIRS_NOAA20_NRT": ("VIIRS_NOAA20_NRT", "N20", "NOAA-20 VIIRS", "VIIRS"),
    "VIIRS_NOAA21_NRT": ("VIIRS_NOAA21_NRT", "N21", "NOAA-21 VIIRS", "VIIRS"),
    "MODIS_TERRA_NRT": ("MODIS_NRT", "T", "Terra MODIS", "MODIS"),
    "MODIS_AQUA_NRT": ("MODIS_NRT", "A", "Aqua MODIS", "MODIS"),
}
MODIS_SOURCES = ("MODIS_TERRA_NRT", "MODIS_AQUA_NRT")
MODIS_FIELDS = FIELDS | {"version"}


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


def parse_row(row, satellite_code, satellite_name, now, instrument="VIIRS"):
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
    if row.get("satellite") != satellite_code or row.get("instrument") != instrument:
        return None
    confidence_percent = None
    if instrument == "MODIS":
        # RT/URT must not establish NRT freshness, even if their measurements are newer.
        if row.get("version") != "6.1NRT":
            return None
        try:
            confidence_percent = float(row.get("confidence", ""))
        except (TypeError, ValueError):
            return None
        if not math.isfinite(confidence_percent) or not 0 <= confidence_percent <= 100:
            return None
        # PlanetaryLens selection classes, NOT NASA probability/class boundaries.
        confidence = "l" if confidence_percent < 30 else "n" if confidence_percent < 80 else "h"
    else:
        confidence = CONFIDENCE_ALIASES.get(str(row.get("confidence", "")).strip().lower())
        if confidence is None:
            return None
    clock = str(row.get("acq_time", "")).strip()
    if not re.fullmatch(r"\d{1,4}", clock):
        return None
    date = str(row.get("acq_date", ""))
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", date):
        return None
    try:
        when = datetime.strptime(date + clock.zfill(4), "%Y-%m-%d%H%M").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    measured = int(when.timestamp() * 1000)
    if measured <= 0 or measured > now + FUTURE_MILLIS:
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
        if confidence_percent is not None:
            record["confidencePercent"] = confidence_percent
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
    return (
        -record["frp"], -record["time"], identity(record),
        -record.get("confidencePercent", 0), record["confidence"], record["lat"], record["lon"],
    )


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


def source_snapshot(source_id, status, latest=None):
    product, _, satellite, instrument = SOURCE_PROFILES[source_id]
    return {
        "product": product,
        "satellite": satellite,
        "instrument": instrument,
        "status": status,
        "latestMillis": latest,
        "selected": False,
    }


def assess_product(payload, product, now):
    """Parse once per transport, retain independent A/T health before point filtering."""
    source_ids = [source for source, profile in SOURCE_PROFILES.items() if profile[0] == product]
    invalid = {source: source_snapshot(source, "invalid") for source in source_ids}
    try:
        if len(payload) > MAX_INPUT_BYTES:
            return invalid, {}
        if not payload.strip() or payload == b"\xef\xbb\xbf":
            return {source: source_snapshot(source, "unavailable") for source in source_ids}, {}
        reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig")), strict=True)
        required = MODIS_FIELDS if product == "MODIS_NRT" else FIELDS
        headers = reader.fieldnames or []
        if not required.issubset(headers) or len(headers) != len(set(headers)):
            return invalid, {}
        latest = dict.fromkeys(source_ids, 0)
        records = {source: [] for source in source_ids}
        seen = dict.fromkeys(source_ids, 0)
        unknown_rows = 0
        codes = {SOURCE_PROFILES[source][1]: source for source in source_ids}
        for row in reader:
            source = codes.get(str(row.get("satellite", "")))
            if source is None:
                unknown_rows += 1
                continue
            seen[source] += 1
            if None in row or any(row.get(field) is None for field in required):
                continue
            _, code, satellite, instrument = SOURCE_PROFILES[source]
            parsed = parse_row(row, code, satellite, now, instrument)
            if parsed is None:
                continue
            measured, point = parsed
            latest[source] = max(latest[source], measured)
            if point is not None:
                records[source].append(point | {
                    "sourceId": source, "product": product, "instrument": instrument,
                })
        health = {}
        for source in source_ids:
            if latest[source]:
                status = "fresh" if latest[source] >= now - SOURCE_MAX_AGE_MILLIS else "stale"
                health[source] = source_snapshot(source, status, latest[source])
            else:
                # A genuinely absent platform is unavailable; rows attributed to it
                # but unusable are invalid. Unknown-only payloads cannot prove absence.
                status = "invalid" if seen[source] or (unknown_rows and not any(seen.values())) else "unavailable"
                health[source] = source_snapshot(source, status)
        return health, records
    except (UnicodeError, csv.Error, TypeError, ValueError):
        # A corrupt transport/schema cannot safely establish either platform's state.
        return invalid, {}


def build_v2(payloads, now, errors=None):
    """Return bounded Schema 2 bytes, or fail without creating a new timestamp."""
    errors = errors or {}
    if (set(payloads) | set(errors)) - set(PRODUCTS) or set(payloads) & set(errors):
        raise ValueError("Unexpected or conflicting NASA product results")
    if type(now) is not int or now <= 0:
        raise ValueError("Invalid generation time")
    health, records = {}, {}
    for product in PRODUCTS:
        if product not in payloads:
            for source, profile in SOURCE_PROFILES.items():
                if profile[0] == product:
                    health[source] = source_snapshot(source, "unavailable")
            continue
        product_health, product_records = assess_product(payloads[product], product, now)
        health.update(product_health)
        records.update(product_records)
    fresh_viirs = [source for source in SOURCES if health[source]["status"] == "fresh"]
    fresh_modis = [source for source in MODIS_SOURCES if health[source]["status"] == "fresh"]
    if len(fresh_viirs) == 2:
        mode, active = "primary_viirs", fresh_viirs
    elif fresh_modis:
        mode, active = "fallback_modis", fresh_modis
    elif fresh_viirs:
        mode, active = "partial_viirs", fresh_viirs
    else:
        raise ValueError("No fresh approved NASA source")
    for source in active:
        health[source]["selected"] = True
    selected, candidates = select([point for source in active for point in records[source]])
    return encode_document({
        "schemaVersion": 2,
        "coverage": "world",
        "generatedAtMillis": now,
        "minFrpMw": MIN_FRP_MW,
        "windowHours": 24,
        "sourceLatestMillis": {source: health[source]["latestMillis"] for source in active},
        "candidateCount": candidates,
        "limited": candidates > len(selected),
        "hotspots": selected,
        "mode": mode,
        "sources": health,
    })


def encode_document(document):
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
    if source not in PRODUCTS or not re.fullmatch(r"[a-fA-F0-9]{32}", key):
        raise ValueError("FIRMS_MAP_KEY is missing or invalid")
    url = f"{NASA_BASE_URL}/api/area/csv/{key}/{source}/world/2"
    with tempfile.TemporaryDirectory(prefix="planetarylens-fire-") as directory:
        output = Path(directory) / "response.csv"
        command = [
            "curl",
            "--disable",
            "--silent",
            "--fail",
            # Azure GitHub runners often have broken IPv6 or HTTP/2 to FIRMS.
            # Either hang consumes the whole connect budget as curl error 28.
            "--ipv4",
            "--http1.1",
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


def download_results(key, products=PRODUCTS):
    """Collect successes AND failures; permanent errors never block another retry."""
    if not products or len(products) != len(set(products)) or set(products) - set(PRODUCTS):
        raise ValueError("Unexpected or conflicting NASA product results")
    payloads, errors = {}, {}
    pending = list(products)
    for round_index in range(2):
        if round_index:
            print("NASA temporary failure: retrying only missing products after cooldown", flush=True)
            time.sleep(SOURCE_RETRY_DELAY_SECONDS)
        # At most three concurrent transports; both recovery rounds are parallel.
        with ThreadPoolExecutor(max_workers=len(products)) as pool:
            futures = {source: pool.submit(download, key, source) for source in pending}
            for source, future in futures.items():
                try:
                    payloads[source] = future.result()
                    errors.pop(source, None)
                except Exception as error:
                    errors[source] = error
        pending = [
            source for source in products if source in errors
            and isinstance(errors[source], DownloadError) and errors[source].retryable
        ]
        if not pending:
            break
    return payloads, errors


def download_all(key):
    # Legacy strict V1 entry point. The V2 CLI uses download_results instead.
    payloads, errors = download_results(key, tuple(SOURCES))
    if errors:
        raise next(iter(errors.values()))
    return payloads


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
        "No fresh approved NASA source",
        "Unexpected or conflicting NASA product results",
        "Invalid generation time",
        "V1 and V2 output paths must differ",
    } | {
        f"NASA {category} for {source}"
        for source in PRODUCTS
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
    """Same-filesystem atomic replacement; failed writes leave no partial/temp feed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=path.name + ".", suffix=".tmp", delete=False) as output:
            temporary = Path(output.name)
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def publish_outputs(payloads, errors, now, output_v1, output_v2):
    if output_v1.resolve() == output_v2.resolve():
        raise ValueError("V1 and V2 output paths must differ")
    # Validate all intended documents before changing either file.
    content_v2 = build_v2(payloads, now, errors)
    document = json.loads(content_v2)
    content_v1 = None
    if document["mode"] == "primary_viirs":
        content_v1 = build({source: payloads[source] for source in SOURCES}, now)
    write_feed(output_v2, content_v2)
    if content_v1 is not None:
        write_feed(output_v1, content_v1)
    return document, len(content_v2), content_v1 is not None


def report_result(document, size, updated_v1, now):
    lines = [
        f"V2 mode: {document['mode']}; {len(document['hotspots'])} / "
        f"{document['candidateCount']} detections; {size} bytes.",
        "V1: updated (strict VIIRS pair)." if updated_v1 else "V1: unchanged; primary VIIRS pair degraded.",
    ]
    for source in SOURCE_PROFILES:
        health = document["sources"][source]
        latest = health["latestMillis"]
        age = "unknown" if latest is None else f"{(now - latest) / 3600000:.3f} h"
        lines.append(f"{source}: {health['status']}, selected={health['selected']}, age={age}.")
    print("\n".join(lines), flush=True)
    if not updated_v1:
        print("::warning::VIIRS primary pair degraded; V2 delivered, V1 retained.", flush=True)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as output:
            output.write("## Fire feed result\n\n" + "\n".join("- " + line for line in lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/fire-hotspots.json"), help="Strict V1 output")
    parser.add_argument("--output-v2", type=Path, default=Path("data/fire-hotspots-v2.json"))
    parser.add_argument("--input-dir", type=Path, help="Local NASA CSV directory for offline verification")
    args = parser.parse_args()
    try:
        if args.output.resolve() == args.output_v2.resolve():
            raise ValueError("V1 and V2 output paths must differ")
        if args.input_dir:
            payloads, errors = {}, {}
            for product in PRODUCTS:
                try:
                    with (args.input_dir / (product + ".csv")).open("rb") as response:
                        content = response.read(MAX_INPUT_BYTES + 1)
                    if len(content) > MAX_INPUT_BYTES:
                        raise ValueError("NASA response exceeds input budget")
                    payloads[product] = content
                except OSError:
                    errors[product] = DownloadError(product, "transport unavailable")
                except ValueError as error:
                    errors[product] = error
        else:
            key = os.environ.get("FIRMS_MAP_KEY", "").strip()
            if not re.fullmatch(r"[a-fA-F0-9]{32}", key):
                raise ValueError("FIRMS_MAP_KEY is missing or invalid")
            payloads, errors = download_results(key)
        now = int(time.time() * 1000)
        document, size, updated_v1 = publish_outputs(payloads, errors, now, args.output, args.output_v2)
        report_result(document, size, updated_v1, now)
    except Exception as error:
        # No raw upstream payload, secret, URL, or traceback in public workflow logs.
        print(
            f"Feed refresh failed: {safe_failure_reason(error)}; no successful publication.", file=sys.stderr
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
