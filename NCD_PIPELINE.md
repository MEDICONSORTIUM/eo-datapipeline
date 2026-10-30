# EOHA NCD environmental pipeline

This pipeline is separate from the working malaria pipeline. It reads the same
`MDB_Wards_2020` ward asset and resolves `WardID` against `eoha.regions.region_code`.
It does not insert or update shared regions and does not write to malaria observations.

## First release

The implemented indicators are:

| Indicator | Source | Meaning |
|---|---|---|
| Mean air temperature and mean daily maximum | ERA5-Land hourly | Modelled 2 m air temperature |
| NO2 | Sentinel-5P OFFL L3 | Tropospheric atmospheric column, not total NOx or ground concentration |
| SO2 | Sentinel-5P OFFL L3 | Atmospheric column, not ground concentration |
| PM2.5 and PM10 | CAMS NRT | Coarse modelled surface concentration |
| PM2.5-10 | CAMS NRT | PM10 minus PM2.5 using matched fields |
| UV Index | CAMS NRT | Mean 12:00 UTC / 14:00 SAST value, derived from biologically effective dose rate |
| Built-up fraction and area | Dynamic World V1 | Annual classified proxy, processed only with a December run |

These values are environmental observations and proxies. They are not NCD diagnoses
or a disease-risk score. Disaster impacts, silica, SAAQIS station data and actual NOx
remain later-phase sources and are not collected by this code.

## Setup

Use Python 3.11 or later and install the existing project requirements:

```powershell
python -m pip install -r requirements.txt
```

Required settings are `GCP_SA_JSON`, `BUCKET_NAME`, `DB_HOST`, `DB_NAME`,
`DB_USER` and `DB_PASSWORD`. `GCP_PROJECT`, `DB_PORT` and `DB_SSLMODE` are
optional. The Google identity needs Earth Engine and Cloud Storage access. The
database role needs `SELECT` on `eoha.regions` and insert/update access to the
four NCD tables and their sequences.

GitHub Actions stores these values as protected environment secrets in an
environment named `ncd-smoke-test`. The workflow has read-only repository
permissions and its schedule is intentionally disabled.

## Migration

Review `migrations/001_create_ncd_schema.sql`, take the normal RDS backup, and apply
it with a database migration role. Do not run the old root `schema.sql`; it describes
retired tables. Example after review:

```powershell
psql "host=<host> dbname=eoha user=<migration-role> sslmode=require" `
  -v ON_ERROR_STOP=1 -f migrations/001_create_ncd_schema.sql
```

Grant the runtime role only `USAGE` on `ncd`, `SELECT` on `eoha.regions`, and the
required NCD table/sequence privileges using the organisation's normal role
management. The migration is prepared but is not applied by this repository.

## Commands

Run all local checks without cloud credentials:

```powershell
python -m compileall -q .
python -m unittest discover -s tests -v
```

After the migration, run a three-ward live smoke test manually:

```powershell
python ncd_pipeline.py --month 2026-01 --province Limpopo --smoke-test
```

A normal manual run uses:

```powershell
python ncd_pipeline.py --month 2026-01
```

If no month is given, the previous completed calendar month is selected. Months before
January 2026 and incomplete current or future months are rejected. December runs also calculate the annual Dynamic World
indicators for that calendar year. Other months do not repeat or relabel annual data.

## Processing and reruns

The path is Earth Engine -> `ncd/` in Cloud Storage -> Python validation -> the
`ncd` schema in RDS. Each CSV has a JSON manifest beside it. Exports use:

`ncd/YYYY-MM/Province/run_<run-id>.csv`

The database uniqueness rule includes ward, indicator, source, actual period,
statistic, processing method and boundary version. A rerun updates that row and
retains the new run ID. A null or rejected rerun value does not erase an existing
valid value. Missing and low-coverage values are stored as SQL `NULL` with a quality
flag when there is no existing valid row; they are never replaced with zero.

The first-release coverage thresholds are 90% for ERA5/CAMS, 20% for Sentinel-5P,
and 20% for Dynamic World scene coverage. Sentinel-5P L3 validity filters are
provided by the catalogue, with an additional cloud-fraction threshold of 0.30.
For Sentinel-5P, expected samples are counted per pixel from the available source
band before the additional cloud filter. Valid samples are counted after that
filter. Both count images include zeros and are averaged over the same ward grid;
coverage is the ratio of those ward means. Images elsewhere in the province do
not inflate the denominator. This is retention of catalogue-valid samples under
the additional cloud filter, not a fraction of calendar days with observations.
The expected sample count and coverage method are retained in processing metadata.
Negative SO2 retrievals that survive source filtering are not changed. Negative
PM or UV values and built-up fractions outside 0-1 are stored as null with an
`OUT_OF_RANGE` flag. CAMS uses
the 00 UTC model cycle and first-day forecast steps only, so repeated five-day
forecasts are not counted as separate observations.

NCD exports include ward-centroid latitude/longitude and retain municipality,
district and coordinates in each observation's `processing_metadata`. This does
not require changes to shared regions or malaria tables. Missing location fields
remain JSON nulls.

After the coverage/location correction, rerun affected months using fresh Earth
Engine exports, starting with a three-ward smoke test. Do not reload older CSVs:
they contain province-wide expected counts and lack coordinates. The existing
`ncd-v1` observation identity is retained so reruns repair rows in place rather
than creating duplicate scoring inputs. Manifests record the corrected coverage
method. Existing database rows are unchanged until a rerun completes.

## Dataset limits

- Sentinel-5P NO2 and SO2 are atmospheric columns. They cannot be reported as
  breathing-level concentrations. NO2 is not total NOx.
- CAMS has an approximately 0.4 degree grid. Ward summaries do not create local
  detail beyond that grid.
- ERA5-Land and CAMS are model products rather than direct measurements.
- Dynamic World is a classified built-up proxy. It does not measure population
  growth and its annual value is preserved at annual resolution.
- A low-coverage or unavailable source produces a null value with an explicit flag.

## Deployment sequence

1. Review and apply the SQL migration through the normal database change process.
2. Create the protected `ncd-smoke-test` GitHub environment and add its secrets.
3. Run local checks again from the deployment commit.
4. Manually dispatch a January 2026, one-province, three-ward smoke test.
5. Review the GCS CSV/manifest, NCD run record, ward links, units and quality flags.
6. Run one full province, then the national January 2026 backfill.
7. Enable the monthly schedule only after those reviews pass.

