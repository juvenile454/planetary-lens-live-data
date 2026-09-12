# PlanetaryLens world fire feed

Small, public NASA FIRMS VIIRS selection for PlanetaryLens. No Android application source,
NASA key, or raw downloads are published here. `build_feed.py` uses Python 3.10+
and curl 8.4+ (both supplied by the Ubuntu 24.04 runner). It is scheduled in
GitHub Actions at minutes 7, 22, 37 and 52 of each hour.
Scheduling can be delayed; every output carries its true generation time.

## Data contract

- NASA FIRMS NOAA-20 and NOAA-21, worldwide, rolling 24 hours in UTC.
- FRP **at least 50 MW**, nominal/high detection confidence.
- At most **400 detections and 256 KiB** after filtering on the job host.
- Selection takes the strongest point from each occupied 30-degree cell in turns,
  preserving sparse regions before taking more detections from dense regions.
- Individual unusable NRT rows are skipped. An unavailable, schema-invalid, empty
  or outdated satellite source still fails the whole refresh. The previous
  published file is retained; the app refuses feeds older than 3 hours.
- These are sampled thermal detections, not confirmed wildfires, perimeters or warnings.

## Access and operation

Set the repository Actions secret `FIRMS_MAP_KEY`, then run **Update world fire feed**.
The job downloads two UTC calendar days per satellite on the runner, filters the
exact last 24 hours, and publishes only `data/fire-hotspots.json`. A NASA key is
never needed by the Android app. No unfiltered world data is sent to the phone.

Run tests with `python3 -m unittest discover -v` (requires curl and OpenSSL;
all transport tests use an ephemeral loopback HTTPS server, never NASA). Run a local refresh with
`FIRMS_MAP_KEY` in the process environment and `python3 build_feed.py`.
`--input-dir` accepts a directory with the two named NASA CSV files for offline
verification. Raw CSV files and credentials must stay outside this repository.

The source of this publisher is maintained in the private app checkout under
`services/fire-feed/`. Copy the Python files and this README to the data repository;
copy `workflow.yml` to `.github/workflows/update-fire-feed.yml`.

## Attribution and rights

NASA FIRMS / LANCE, NASA and NOAA. VIIRS 375 m active fire products:
[NASA API documentation](https://firms.modaps.eosdis.nasa.gov/api/area/),
[FIRMS map](https://firms.modaps.eosdis.nasa.gov/map/),
[NASA data use and citation guidance](https://www.earthdata.nasa.gov/engage/open-data-services-software/data-use-policy).
NASA Earth science data may be reused and redistributed under the cited policy.
No NASA logo, satellite imagery, third-party map tiles or private user data are included.

NOAA-20 product: https://doi.org/10.5067/FIRMS/VIIRS/VJ114IMGT_NRT.002
NOAA-21 product: https://doi.org/10.5067/VIIRS/VJ214IMGTDL_NRT.002

S-NPP is not a required source because NASA announced the end of its product delivery
on November 1, 2026. NOAA-20 and NOAA-21 provide the global inputs used here.

## Reliability and diagnostics

- Each request has a 10-second connection limit, a **45-second total transfer limit**,
  and a 50-second subprocess guard that kills and reaps a stuck curl process.
  Unlike a socket inactivity timeout, these limits also stop slowly trickling responses.
- Each satellite gets three attempts (2/4-second backoff). After exhaustion of a
  temporary failure, wait 30 seconds and retry **only** the missing satellite(s).
  Both rounds run in parallel: the worst-case transport budget is 342 seconds,
  not multiplied by the number of satellites. The build step has a 7-minute guard;
  the job retains its 12-minute limit with headroom for tests and publication.
- HTTP 408/409/425/429/500/502/503/504, DNS/connect errors, incomplete transfers
  and transient transport failures can retry. HTTP 401/403, redirects, TLS trust
  failures, oversize input and missing tools fail without an outer retry.
- curl configuration is disabled; the key-bearing URL goes over stdin, never the
  command line. HTTPS certificate validation stays enabled; redirects are not followed.
  Temporary raw files are private to an attempt and cleaned on success/failure.
- Logs contain only fixed source names, HTTP status/curl code, duration and byte
  counts. Common curl codes: 6 DNS, 7 connect, 18 truncated response, 28 timeout,
  56 receive/reset, 60 TLS certificate, 63 size budget. Never add raw stderr,
  upstream response bodies, request URLs, secrets or curl verbose tracing to logs.
- Failed transport/schema/freshness validation never changes the public output or
  renews its timestamp. Both satellites must still be fresh within 12 hours.
- Pull requests run the offline regression suite with read-only permissions and
  no NASA secret. Only main can publish; queued jobs check out current main.
  Publishing is a normal fast-forward push followed by an exact remote blob check.
  A concurrent edit that prevents a fast-forward fails safely: rerun on current
  main after reviewing the edit; never force-push a stale feed over it.

### Verification and incident handling

After a change, run the full Python suite, then the Actions workflow on main.
Verify both the run's publication step and the JSON at the **actual app URL**:
`https://raw.githubusercontent.com/juvenile454/planetary-lens-live-data/main/data/fire-hotspots.json`.
Check generation time, both `sourceLatestMillis` values, filters and size/count
limits. GitHub's raw CDN can briefly serve a previous version; a green run alone
is not proof that devices see the new blob. Compare against the exact main
Contents API response, not a fabricated timestamp or a modified app URL.

If NASA remains unavailable after the bounded attempts, the run must be red and
the last valid file remains. Do not hide the outage with `continue-on-error`, a
partial satellite feed or a rewritten timestamp. Diagnose the logged status/code;
a 401/403 needs a key/access check, while TLS errors must never be bypassed.

The 15-minute GitHub schedule is best-effort and can be delayed by hours. The
existing external freshness watchdog dispatches main when the published feed is
older than 25 minutes and alerts from 90 minutes. It must remain independent of
this workflow. Scheduling/hosting and sustained NASA outages cannot be guaranteed
away by retries; production still needs independent freshness monitoring.
