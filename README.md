# PlanetaryLens world fire feed

Small, public NASA FIRMS VIIRS selection for PlanetaryLens. No Android application source,
NASA key, or raw downloads are published here. `build_feed.py` uses only Python's
standard library and is scheduled in GitHub Actions at minutes 7, 22, 37 and 52 of each hour.
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

Run tests with `python3 -m unittest -v test_build_feed.py`. Run a local refresh with
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

Temporary HTTP 429/5xx and transport failures are retried at most five times with short bounded delays, and both satellites are downloaded in parallel. Authentication failures and a missing/outdated satellite still fail without publishing. Public diagnostics distinguish timeouts, HTTP failures and connection failures using only fixed messages, never upstream URLs or payloads. The 15-minute GitHub schedule is best-effort: public repository cron jobs are often delayed by hours, so a local watchdog may dispatch this workflow when the published file grows stale. GitHub scheduling delays remain possible and require freshness monitoring for production operation.
