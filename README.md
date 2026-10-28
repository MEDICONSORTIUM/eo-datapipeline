## Current EOHA database connection

Install dependencies with `python -m pip install -r requirements.txt`, then run
`python database.py` from this folder. The connection loads `.env` beside
`database.py`; existing environment variables take precedence. Keep `.env`
out of Git (it is ignored).

Set `DB_HOST` to the RDS hostname, `DB_NAME=eoha`, `DB_USER` to your database user,
and `DB_PASSWORD` to your password. Optional settings are `DB_PORT=5432` and
`DB_SSLMODE=require`.

The check uses a read-only transaction and validates columns and table write
permissions for `eoha.data_sources`, `eoha.environmental_variables`,
`eoha.regions`, `eoha.environmental_observations`, and `eoha.etl_runs`.
Connections use `search_path=eoha,public`. The check does not insert or change data.

Do not use `--init`: it is disabled because `schema.sql` describes the old
`public.zone` and `public.malaria_observation` tables, not the schema in RDS.

`etl_pipeline.py` writes wards to `eoha.regions`, stores each measurement in
`eoha.environmental_observations`, and records every attempt in `eoha.etl_runs`.
It also keeps the source and variable reference rows up to date.

Before a full run, process three wards with:

```powershell
python etl_pipeline.py --smoke-test --province Limpopo
```

Use `--month YYYY-MM` to select a month. A smoke-test CSV is removed after it is
loaded. Normal monthly CSV exports remain in the bucket.

Run local tests with `python -m unittest discover -s tests -v`.
These use mocks and need no database or Google credentials. The older project
description below documents the legacy pipeline; its separate testable module
and advertised 98-test suite are absent from this checkout.

---

<div align="center">

<br />

<img src="https://img.shields.io/badge/-Google_Earth_Engine-34A853?style=for-the-badge&logo=google&logoColor=white" />
<img src="https://img.shields.io/badge/-Google_Cloud_Storage-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white" />
<img src="https://img.shields.io/badge/-PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white" />
<img src="https://img.shields.io/badge/-Python_3.12-3776AB?style=for-the-badge&logo=python&logoColor=white" />
<img src="https://img.shields.io/badge/-Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" />
<img src="https://img.shields.io/badge/-Pytest-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white" />

<h2 align="center">EOHA EO Data Pipeline</h2>

<div align="center">
Automated monthly pipeline that extracts satellite and reanalysis data from Google Earth Engine for all South African provinces and ingests per-ward environmental indicators into an AWS RDS PostgreSQL database — powering the EOHA (Earth Observational Health Analytics) malaria risk platform.
</div>

<br />

**MED Consortium · EOHA Platform · PI: Kagisho Montjane**
*Funded by TuksNovation and NeoFrontiers*

</div>

---

## 📋 Table of Contents

1. [Introduction](#introduction)
2. [Tech Stack](#tech-stack)
3. [Pipeline Architecture](#pipeline-architecture)
4. [Repository Structure](#repository-structure)
5. [Data Sources](#data-sources)
6. [Database Schema](#database-schema)
7. [Quick Start](#quick-start)
8. [Running the Tests](#running-the-tests)
9. [Environment Variables](#environment-variables)
10. [Deployment](#deployment)
11. [Contributing](#contributing)

---

## 🤖 Introduction <a name="introduction"></a>

The EOHA EO Data Pipeline is a scheduled batch job that runs once per month to ingest satellite-derived environmental risk factors into a PostgreSQL database. These indicators — land surface temperature, vegetation index, water index, soil moisture, population density, and land cover — are used downstream by the EOHA platform to model malaria transmission risk at ward level across South Africa.

The pipeline processes **all 9 South African provinces** in parallel batch export tasks on Google Earth Engine, then sequentially downloads and ingests each province's ward-level CSV into the database. The entire process is idempotent: re-running the pipeline for the same month produces the same database state.

**Key design decisions:**

- Business logic is extracted into testable pure functions in `eo-datapipeline.py`. The original `etl_pipeline.py` script remains the production entry point but delegates all transform logic to the module.
- All 98 automated tests run **without any GCP credentials** — external surfaces are mocked at the module boundary.
- Every PostgreSQL write uses `ON CONFLICT DO UPDATE` to ensure safe re-runs.

---

## ⚙️ Tech Stack <a name="tech-stack"></a>

| Layer | Technology |
|-------|-----------|
| Satellite data | Google Earth Engine (MODIS, ERA5, WorldPop, ESA WorldCover) |
| Cloud storage | Google Cloud Storage (CSV export staging area) |
| Data transforms | Python 3.12 · Pandas |
| Database writes | psycopg2 · PostgreSQL (AWS RDS) |
| Authentication | GCP Service Account (JSON key via environment variable) |
| Testing | pytest 9.0.3 · unittest.mock |
| Scheduling | Google Cloud Run (monthly trigger) |

---

## 🏗️ Pipeline Architecture <a name="pipeline-architecture"></a>

The pipeline runs in 8 sequential steps. Steps 5–8 loop over all 9 provinces.

```
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 0 — Authentication                                            │
│  GCP service account JSON → ee.Initialize + storage.Client         │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────┐
│  STEP 1 — Month Resolution                                          │
│  Derive previous calendar month as the processing window            │
│  month_label = e.g. "2025-03"                                       │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────┐
│  STEP 2 — Asset Loading                                             │
│  Ward boundaries · WorldPop 2020 · ESA WorldCover v100             │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │  For each of 9 provinces:            │
              │                                      │
              │  STEP 3 — Image Composition          │
              │  build_safe_band_image() ×4          │
              │  LST · NDVI · NDWI · ERA5 stack      │
              │                                      │
              │  STEP 4 — Region Reduction           │
              │  reduceRegions(mean, 1000m)           │
              │  reduceRegions(mode, 1000m) [habitat] │
              │  Join + add lat/lon/pop_density       │
              │                                      │
              │  STEP 5 — Export Task Submission     │
              │  ee.batch.Export → GCS CSV           │
              └──────────────────┬──────────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │  For each submitted task:            │
              │                                      │
              │  STEP 6 — Task Polling (30s interval)│
              │  poll_task_until_complete()          │
              │  COMPLETED → continue                │
              │  Any other state → sys.exit(1)       │
              │                                      │
              │  STEP 7 — Transform                  │
              │  load_csv_from_gcs()                 │
              │  build_zone_dataframe()              │
              │  build_observation_dataframe()       │
              │                                      │
              │  STEP 8 — DB Upsert                  │
              │  upsert_zone_rows()                  │
              │  upsert_observation_rows()           │
              └─────────────────────────────────────┘
```

---

## 📁 Repository Structure <a name="repository-structure"></a>

```
/
├── etl_pipeline.py                   # Production entry point (monolithic script)
├── eo-datapipeline.py                # Testable module (pure functions only)
├── requirements.txt                  # Python dependencies
│
├── tests/
│   ├── conftest.py                   # Shared pytest fixtures (all modules)
│   ├── test_unit.py                  # GEE composition layer — 13 tests
│   ├── test_integration_transforms.py  # DataFrame transforms + GCS — 27 tests
│   ├── test_integration_db.py        # PostgreSQL upsert logic — 23 tests
│   └── test_e2e_pipeline.py          # Full pipeline flow — 35 tests
│
├── README.md                         # This file
├── Test.md                           # Full test suite documentation
└── Agent.md                          # LLM agent guide and coding ideology
```

---

## 🛰️ Data Sources <a name="data-sources"></a>

| Indicator | GEE Collection | Band | Transform |
|-----------|---------------|------|-----------|
| Land Surface Temp | `MODIS/061/MOD11A1` | `LST_Day_1km` | ×0.02 − 273.15 → °C |
| Vegetation Index | `MODIS/061/MOD13Q1` | `NDVI` | ×0.0001 → dimensionless |
| Water Index | `MODIS/061/MOD09GA` | `sur_refl_b04`, `sur_refl_b02` | Normalised difference |
| Air Temperature | `ECMWF/ERA5_LAND/HOURLY` | `temperature_2m` | − 273.15 → °C |
| Soil Moisture | `ECMWF/ERA5_LAND/HOURLY` | `volumetric_soil_water_layer_1` | Raw (m³/m³) |
| Population Count | `WorldPop/GP/100m/pop` | `population` | ×100 → per km² |
| Agricultural land | `ESA/WorldCover/v100` | `Map` | Binary mask class 40 |
| Habitat Class | `ESA/WorldCover/v100` | `Map` | Mode per ward |
| Ward Boundaries | `projects/modis-temperature-data/assets/MDB_Wards_2020` | — | FeatureCollection |

All satellite bands use `build_safe_band_image()` which returns a blank masked image if no scenes exist for the processing window — preventing crashes on sparse collections.

---

## 🗄️ Database Schema <a name="database-schema"></a>

### `zone` table

One row per South African ward. Updated monthly.

| Column | Type | Description |
|--------|------|-------------|
| `zone_id` | `INTEGER PRIMARY KEY` | Numeric ward ID (e.g. `79700001`) |
| `zone_name` | `TEXT` | Ward label (e.g. `'Ward 1 - Musina'`) |
| `municipality_name` | `TEXT` | Municipality name |
| `province_name` | `TEXT` | Province (e.g. `'Limpopo'`) |
| `base_population` | `INTEGER` | WorldPop 2020 count; `0` if null |
| `dominant_habitat_code` | `TEXT` | ESA WorldCover mode class code |
| `external_zone_id` | `TEXT` | String version of ward ID |
| `zone_type` | `TEXT` | Always `'Ward'` |
| `latitude` | `FLOAT` | Centroid latitude |
| `longitude` | `FLOAT` | Centroid longitude |
| `population_density_per_km2` | `FLOAT` | WorldPop count × 100; `NULL` if missing |

### `malaria_observation` table

One row per ward per month. Composite primary key: `(zone_id, date_recorded)`.

| Column | Type | Description |
|--------|------|-------------|
| `date_recorded` | `DATE` | Always the 1st of the month |
| `zone_id` | `INTEGER` | FK → `zone.zone_id` |
| `lst_surface_temp_c` | `FLOAT` | MODIS land surface temperature (°C) |
| `air_temp_c` | `FLOAT` | ERA5 2m air temperature (°C) |
| `soil_moisture_pct` | `FLOAT` | ERA5 volumetric soil water (m³/m³) |
| `ndvi_vegetation_index` | `FLOAT` | MODIS NDVI |
| `ndwi_water_index` | `FLOAT` | MODIS NDWI |
| `agric_percentage` | `FLOAT` | Fraction of ward area as cropland |

---

## 🤸 Quick Start <a name="quick-start"></a>

### Prerequisites

- Python 3.12+
- A GCP service account JSON with `earthengine` and `cloud-platform` scopes
- A GCS bucket for CSV staging
- A PostgreSQL database (AWS RDS or local)

### Clone the Repository

```bash
git clone https://github.com/med-consortium/eoha-pipeline.git
cd eoha-pipeline
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

```
earthengine-api>=0.1.400
google-cloud-storage
pandas
psycopg2-binary
google-auth>=2.20.0
```

### Set Environment Variables

```bash
export GCP_SA_JSON='{ ... full service account JSON ... }'
export BUCKET_NAME='your-gcs-bucket-name'
export DB_HOST='your-rds-endpoint.rds.amazonaws.com'
export DB_NAME='eoha_db'
export DB_USER='pipeline_user'
export DB_PASSWORD='your-password'
```

### Run the Pipeline

```bash
python etl_pipeline.py
```

The pipeline will log progress at each step:

```
2025-03-01 02:00:01 - INFO - Running pipeline for month: 2025-02
2025-03-01 02:00:03 - INFO - Export task started for: Limpopo
2025-03-01 02:00:04 - INFO - Export task started for: Gauteng
...
2025-03-01 02:04:12 - INFO - Task finished: Limpopo_2025-02. Loading into database...
2025-03-01 02:04:15 - INFO - Successfully loaded into database: Limpopo_2025-02
...
2025-03-01 02:18:44 - INFO - Monthly pipeline complete.
```

---

## 🧪 Running the Tests <a name="running-the-tests"></a>

No GCP credentials required. All 98 tests run in under 1 second.

### Install Test Dependencies

```bash
pip install pytest pandas psycopg2-binary
```

### Run All Tests

```bash
pytest tests/ -v --tb=short
```

### Run by Layer

```bash
# GEE composition logic only
pytest test_unit.py -v

# DataFrame transforms + GCS loader
pytest test_integration_transforms.py -v

# PostgreSQL upsert logic
pytest test_integration_db.py -v

# Full end-to-end flow
pytest test_e2e_pipeline.py -v
```

### Expected Output

```
========================= 98 passed in 0.82s ==========================
```

For full test documentation including what each test verifies and how to write new tests, see [Test.md](./Test.md).

---

## 🔑 Environment Variables <a name="environment-variables"></a>

| Variable | Required | Description |
|----------|----------|-------------|
| `GCP_SA_JSON` | Yes | Full JSON string of GCP service account credentials |
| `BUCKET_NAME` | Yes | GCS bucket name where pipeline CSVs are staged |
| `DB_HOST` | Yes | PostgreSQL host (AWS RDS endpoint) |
| `DB_NAME` | Yes | Database name |
| `DB_USER` | Yes | Database user |
| `DB_PASSWORD` | Yes | Database password |

---

## 🚀 Deployment <a name="deployment"></a>

The pipeline is deployed as a **Google Cloud Run job** triggered monthly by Cloud Scheduler.

```
Cloud Scheduler (monthly cron)
    → Cloud Run Job trigger
        → Docker container: python etl_pipeline.py
            → GEE (batch export tasks)
            → GCS (CSV staging)
            → AWS RDS (PostgreSQL upsert)
```

The container reads all credentials from environment variables injected at runtime via Cloud Run secret bindings.

---

## 🤝 Contributing <a name="contributing"></a>

1. All new business logic must be added to `eo-datapipeline.py` as a pure function — not inline in `etl_pipeline.py`.
2. Every new function requires at least one test in the appropriate test module.
3. Follow the docstring format documented in [Agent.md](./Agent.md).
4. Run `pytest tests/ -v` and confirm all 98 tests pass before opening a pull request.
5. Do not commit credentials, `.env` files, or GCP service account JSON.

---

<div align="center">

EOHA Platform · MED Consortium
Authors: Msesenyane Makhongela, Simamnkele Mlisana

</div>
