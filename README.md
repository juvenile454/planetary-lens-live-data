# PlanetaryLens world fire feed

Bounded public NASA LANCE FIRMS selection for PlanetaryLens. Python 3.10+,
curl 8.4+ and OpenSSL are supplied by the Ubuntu 24.04 Actions runner.
NASA credentials and raw CSV stay on the publisher host, never in Android or
public JSON. The existing repository Actions secret `FIRMS_MAP_KEY` is used.

## Versioned outputs

- `data/fire-hotspots.json`: Schema 1, strict fresh NOAA-20 **and** NOAA-21 VIIRS.
- `data/fire-hotspots-v2.json`: Schema 2, explicit source health and selected mode.

The publisher fetches three fixed Area-API `world/2` products in parallel:
`VIIRS_NOAA20_NRT`, `VIIRS_NOAA21_NRT`, `MODIS_NRT`. MODIS Terra (`Terra`/`T`) and Aqua
(`Aqua`/`A`) are assessed independently using an exact closed satellite mapping,
requiring actual `instrument = MODIS` and
`version = 6.1NRT`; missing fields and RT/URT never establish NRT freshness.

Deterministic priority (no sensor mixing):

1. Both VIIRS sources fresh: `primary_viirs`, renew V1 and V2.
2. Otherwise any MODIS platform fresh: `fallback_modis`, select all fresh MODIS
   platforms, renew V2 only; V1 bytes and generation time remain unchanged.
3. Otherwise one VIIRS source fresh: `partial_viirs`, renew V2 only.
4. No fresh approved source: fail and retain both previous outputs.

V2 lists exactly four fixed profiles with product, satellite, instrument,
`fresh`/`stale`/`unavailable`/`invalid`, measurement time and explicit selection.
Selected source timestamps and every point's source metadata must agree.
Degraded success emits an Actions warning and summary; it is not primary health.

## Selection and freshness

- Rolling 24-hour UTC observations, at most 5 minutes in the future; raw source
  maxima must be within 12 hours, evaluated before confidence/FRP filtering.
- FRP >=150 MW; VIIRS nominal/high confidence. MODIS confidence must be finite
  0..100; selection >=30, with retained `confidencePercent`. Mapping 30..<80 to
  nominal and 80..100 to high is a PlanetaryLens selection convention, not a NASA
  probability or asserted class boundary.
- At most 400 points and 256 KiB; deterministic deduplication and round-robin
  selection across occupied 30-degree cells preserve sparse regions.
- Thermal observations, not confirmed wildfires, perimeters, warnings or proof
  that empty areas are fire-free. MODIS 1000 m pixels are not VIIRS 375 m pixels.
- Generation, source and observation timestamps remain distinct. Consumers must
  enforce <=3-hour feed age and <=12-hour active source age at fetch.

## Operation and verification

`.github/workflows/update-fire-feed.yml` preserves best-effort scheduling at
minutes 7/22/37/52, read-only offline PR tests, main-only publication and at most
one second-runner retry on total build failure. Explicitly dispatch on `main`;
check queued/running jobs first. Normal fast-forward push only, followed by exact
remote blob checks for existing outputs. No force-push or timestamp washing.

Requests use validated fixed HTTPS hosts/products, no redirects, IPv4/HTTP1.1,
20-second connect, 45-second transfer and 50-second process limits, 20 MiB per
product. Three attempts with 2/4-second backoff; after 30 seconds, retry only
temporary failures. Both rounds are parallel (342-second worst-case transport).
Permanent HTTP/TLS/auth errors do not retry. Logs never expose keys, key-bearing
URLs, raw CSV or curl stderr. Both intended documents are validated before any
writes; outputs are independent atomic replacements.

Offline tests: `python3 -m unittest discover -v`. Loopback HTTPS and synthetic
fixtures only, no NASA access. `--input-dir` supports bounded offline input using
the three exact product names plus `.csv`; keep genuine raw inputs outside this
repository. Private app publisher source is maintained under `services/fire-feed/`.

Read back both exact main blobs and public URLs; green CI alone is insufficient:

- https://raw.githubusercontent.com/juvenile454/planetary-lens-live-data/main/data/fire-hotspots-v2.json
- https://raw.githubusercontent.com/juvenile454/planetary-lens-live-data/main/data/fire-hotspots.json

Validate public bytes, generation/source ages, mode and selection limits. On a
degraded publication compare V1 against its pre-run hash; stale V1 is expected.
The external watchdog is not changed here: monitoring must separately recognize
usable degraded V2, primary-pair health and stale V1. GitHub schedules may drift.

## Attribution and rights

NASA LANCE FIRMS; PlanetaryLens filtering/JSON preparation. No NASA endorsement,
logos, imagery, third-party tiles or user data. NASA mission data is unrestricted
under the cited guidance (CC0); API access rules and LANCE limitations still apply.

- https://www.earthdata.nasa.gov/learn/use-data/data-citations-acknowledgements
- https://www.earthdata.nasa.gov/data/projects/lance/disclaimer
- https://www.earthdata.nasa.gov/data/catalog/lancemodis-mcd14dl-6.1nrt
- NOAA-20: https://doi.org/10.5067/FIRMS/VIIRS/VJ114IMGT_NRT.002
- NOAA-21: https://doi.org/10.5067/VIIRS/VJ214IMGTDL_NRT.002
- MODIS: https://doi.org/10.5067/FIRMS/MODIS/MCD14DL.NRT.0061
- https://firms.modaps.eosdis.nasa.gov/api/area/
- https://firms.modaps.eosdis.nasa.gov/map/

MODIS is a conditional short-term path within the same FIRMS failure domain,
not a durable independent backup; recheck Terra/Aqua retirement and API status.
S-NPP is not an input.
