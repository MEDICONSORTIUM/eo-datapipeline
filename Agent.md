# 🤖 Agent Guide — EOHA EO Data Pipeline

This document tells any LLM agent exactly how to read, extend, and test this codebase. Read it fully before writing any code.

---

## Project Identity

This is the **EOHA (Earth Observational Health Analytics) EO Data Pipeline** — a monthly batch job that:

1. Authenticates with Google Earth Engine (GEE) and Google Cloud Storage (GCS)
2. Builds satellite image stacks for each South African province
3. Reduces them to per-ward statistics using GEE batch export tasks
4. Downloads the resulting CSVs from GCS
5. Upserts `zone` and `malaria_observation` rows into a PostgreSQL database on AWS RDS

The pipeline runs on a schedule (Cloud Run) and processes data for the **previous calendar month**.

---

## Repository Layout

```
/
├── etl_pipeline.py                # Original monolithic script (production entry point)
├── eo-datapipeline.py             # Testable module: all business logic as pure functions
├── requirements.txt               # Python dependencies
├── tests/
│   ├── conftest.py                # Shared pytest fixtures
│   ├── test_unit.py               # GEE composition layer (unit)
│   ├── test_integration_transforms.py  # DataFrame transforms + GCS (integration)
│   ├── test_integration_db.py     # PostgreSQL upsert (integration)
│   └── test_e2e_pipeline.py       # Full pipeline flow (E2E)
├── README.md
├── Test.md                        # Detailed test documentation
└── Agent.md                       # This file
```

---

## Architecture and Data Flow

```
etl_pipeline.py
│
├── STEP 0  — GCP service account auth (ee.Initialize + storage.Client)
├── STEP 1  — Derive month window (start_date, end_date, month_label)
├── STEP 2  — Load shared GEE assets (wards, WorldPop, ESA land cover)
│
├── STEP 3  — build_safe_band_image(collection, band, scale, offset, output)
│             Wraps any ImageCollection band in an ee.Algorithms.If guard.
│             Returns a blank masked image if no data exists for the month.
│
├── STEP 4  — process_province(province_name)
│             Stacks LST, NDVI, NDWI, ERA5 temp, ERA5 soil moisture,
│             WorldPop population, and ESA cropland into one multi-band image.
│             Calls reduceRegions() (mean at 1000m) for all environmental bands
│             and reduceRegions() (mode at 1000m) for habitat class.
│             Joins and enriches features with lat/lon/habitat/population_density.
│
├── STEP 5  — Submit one ee.batch.Export.table.toCloudStorage task per province.
│             All 9 tasks are submitted before polling begins.
│
├── STEP 6  — Poll each task (every 30s). On COMPLETED: load CSV from GCS.
│             On any other terminal state: log error and sys.exit(1).
│
├── STEP 7  — build_zone_dataframe(df)        → zone table rows
│             build_observation_dataframe(df) → malaria_observation rows
│
└── STEP 8  — upsert_zone_rows(cursor, df_zone)
              upsert_observation_rows(cursor, df_obs)
              Both use ON CONFLICT DO UPDATE (idempotent re-runs).
```

---

## The Two Python Files

### `etl_pipeline.py`

The **original production script**. It has top-level side-effects — `ee.Initialize` and `storage.Client` are called at import time, which requires GCP credentials. Do not import this file in tests.

### `eo-datapipeline.py` (testable module)

All business logic extracted as **pure functions** with no top-level side-effects. This is what tests import. The functions are:

| Function | Inputs | Output | Side-effects |
|----------|--------|--------|-------------|
| `build_zone_dataframe(df)` | raw GEE CSV DataFrame | cleaned `zone` DataFrame | none |
| `build_observation_dataframe(df)` | raw GEE CSV DataFrame | cleaned `malaria_observation` DataFrame | none |
| `upsert_zone_rows(cursor, df_zone)` | psycopg2 cursor, zone DataFrame | int (row count) | SQL via execute_batch |
| `upsert_observation_rows(cursor, df_obs)` | psycopg2 cursor, obs DataFrame | int (row count) | SQL via execute_batch |
| `load_csv_from_gcs(client, bucket, filename)` | GCS client, bucket name, filename slug | pd.DataFrame | GCS network call |
| `poll_task_until_complete(task, filename, poll_interval)` | EE task, filename, int | bool | time.sleep |

---

## Coding Ideology

### 1. No side-effects at module level

Any code that touches GCP, GEE, or environment variables **must** be behind an explicit function call or `if __name__ == '__main__'` guard. If a module initialises clients at import time, it is not testable. Always extract business logic into pure functions first.

### 2. Pure functions over classes

Transform logic (`build_zone_dataframe`, `build_observation_dataframe`) takes a DataFrame in and returns a DataFrame out. No global state. No class instance. This makes them trivially testable and composable.

### 3. Explicit dependency injection

External clients (`storage_client`, `cursor`) are **passed in** as parameters, never instantiated inside transform or upsert functions. Tests inject mocks; production code injects the real clients.

```python
# ✅ CORRECT — client is injected
def load_csv_from_gcs(storage_client, bucket_name, filename):
    bucket = storage_client.bucket(bucket_name)
    ...

# ❌ WRONG — client created inside, untestable
def load_csv_from_gcs(bucket_name, filename):
    client = storage.Client()   # can't mock this
    ...
```

### 4. errors='coerce' for all numeric conversions

GEE CSV exports can contain unexpected string values in numeric columns. Always use `pd.to_numeric(..., errors='coerce')` to convert to `NaN` on failure rather than crashing the pipeline.

```python
# ✅ CORRECT
zone_id = pd.to_numeric(df['WardID'], errors='coerce')

# ❌ WRONG — crashes on non-numeric WardID
zone_id = df['WardID'].astype(int)
```

### 5. Null-safe output

All DataFrames returned from transform functions must end with:
```python
result = result.where(pd.notnull(result), None)
```
This replaces `NaN` with Python `None` so that psycopg2 sends SQL `NULL` rather than the string `'nan'`. This is the last line of every transform function.

### 6. ON CONFLICT DO UPDATE (upserts, never plain inserts)

All PostgreSQL writes use `INSERT ... ON CONFLICT DO UPDATE`. The pipeline must be **idempotent** — re-running it for the same month must produce the same database state, not duplicate rows or errors.

### 7. `execute_batch` with `page_size=1000` for observation rows

Observation rows can be numerous. Always pass `page_size=1000` to `execute_batch` for the `malaria_observation` table to avoid memory issues on large provinces.

### 8. Province slugs use underscores

Province names with spaces (`"North West"`, `"Free State"`) are slugified with `.replace(' ', '_')` when used in filenames. The `PROVINCES` list uses the original spaced names; slugification happens at export time.

---

## How to Write New Tests

### The non-negotiable rules

1. **Never import `ee` or `google.cloud` directly in tests.** Always mock them.
2. **Every test class targets one function.** Name it `TestFunctionName`.
3. **Every test method tests one concern.** Name it `test_what_it_asserts`.
4. **Set `poll_interval=0`** when calling `poll_task_until_complete` to avoid `time.sleep` blocking.
5. **Add the docstring block** to every test function (see format below).
6. **Add shared fixtures to `conftest.py`**, not inside the test file.

---

### Docstring Format (Required on Every Test)

Follow this exact pattern — mirroring the function comment style used throughout the project:

```python
def test_my_behaviour(self, sample_df):
    """
    test_my_behaviour()
    --------------------
    One sentence stating what is being verified and why it matters.
    Reference the specific business rule or data contract being enforced.

    Parameters:
    -----------
    sample_df : pd.DataFrame
        The canonical two-row GEE export fixture.

    Returns:
    --------
    void — asserts <the specific thing>.
    """
    result = function_under_test(sample_df)
    assert result['some_column'].dtype == float
```

If the test takes no fixtures:

```python
def test_provinces_constant_length(self):
    """
    test_provinces_constant_length()
    ---------------------------------
    Ensures the PROVINCES list has exactly 9 entries — one per South
    African province. A mismatch means a province was accidentally added
    or removed.

    Parameters:
    -----------
    none

    Returns:
    --------
    void — asserts len(PROVINCES) == 9.
    """
    assert len(PROVINCES) == 9
```

---

### Anatomy of a New Integration Test

```python
class TestMyNewFunction:
    """Tests for my_new_function() — what it does in one sentence."""

    def test_output_columns_correct(self, sample_df):
        """
        test_output_columns_correct()
        ------------------------------
        Describes why this column set is the contract that downstream
        code depends on.

        Parameters:
        -----------
        sample_df : pd.DataFrame

        Returns:
        --------
        void — asserts expected column set is a subset of result.columns.
        """
        result = my_new_function(sample_df)
        assert {'col_a', 'col_b'}.issubset(set(result.columns))

    def test_row_count_preserved(self, sample_df):
        """
        test_row_count_preserved()
        ---------------------------
        No rows should be added or silently dropped unless the function
        explicitly documents filtering behaviour.

        Parameters:
        -----------
        sample_df : pd.DataFrame

        Returns:
        --------
        void — asserts len(result) == len(sample_df).
        """
        result = my_new_function(sample_df)
        assert len(result) == len(sample_df)
```

---

### Anatomy of a New DB Test

```python
class TestMyNewUpsert:

    def test_sql_targets_correct_table(self, sample_df, mock_db_cursor):
        """
        test_sql_targets_correct_table()
        ---------------------------------
        Verifies that the INSERT statement names the correct table.
        A typo in the table name would cause a runtime error that only
        surfaces in production.

        Parameters:
        -----------
        sample_df : pd.DataFrame
        mock_db_cursor : MagicMock

        Returns:
        --------
        void — asserts 'INSERT INTO my_table' in sql.
        """
        df = build_something(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            my_upsert_function(mock_db_cursor, df)
            sql = mock_eb.call_args[0][1]
            assert 'INSERT INTO my_table' in sql
```

---

### Mocking a New External Service

If you add a new external dependency (e.g. a new GCS client, a new API), follow this pattern:

```python
@pytest.fixture
def mock_new_service():
    """
    mock_new_service()
    -------------------
    MagicMock for the new external service. Configures the happy path.
    Add to conftest.py so all test modules can share it.

    Parameters:
    -----------
    none

    Returns:
    --------
    MagicMock — pre-configured with .method.return_value set.
    """
    service = MagicMock()
    service.some_method.return_value = expected_value
    return service
```

---

## What NOT to Do

| Anti-pattern | Why | Correct approach |
|---|---|---|
| Import `ee` in tests | Triggers GEE auth at import time | Mock with `MagicMock()` |
| Call `storage.Client()` in tests | Requires GCP credentials | Inject `mock_gcs_client` fixture |
| Use `psycopg2.connect()` in tests | Requires live database | Patch `execute_batch` at the module boundary |
| Assert on exact float values without tolerance | Float representation varies | Use `abs(result - expected) < 1e-6` |
| Write tests that depend on execution order | Fragile, hard to debug | Each test must be fully self-contained |
| Use `time.sleep` with real intervals in tests | Slows the suite | Always pass `poll_interval=0` |
| Create fixtures inside test methods | Not reusable | Put shared fixtures in `conftest.py` |
| Test GEE pixel values | Requires live credentials | Test API call arguments only |

---

## Environment Variables (Production Only)

These are never needed to run tests. They are documented here so the pipeline script is understandable.

| Variable | Purpose |
|---|---|
| `GCP_SA_JSON` | Full JSON of GCP service account (GEE + GCS scopes) |
| `BUCKET_NAME` | GCS bucket name for CSV exports |
| `DB_HOST` | PostgreSQL host (AWS RDS endpoint) |
| `DB_NAME` | Database name |
| `DB_USER` | Database username |
| `DB_PASSWORD` | Database password |

---

## Database Schema

### `zone` table

| Column | Type | Notes |
|--------|------|-------|
| `zone_id` | INTEGER PRIMARY KEY | Ward ID from MDB_Wards_2020 |
| `zone_name` | TEXT | Ward label |
| `municipality_name` | TEXT | From `Municipali` column |
| `province_name` | TEXT | Full province name |
| `base_population` | INTEGER | WorldPop 2020 count; 0 if null |
| `dominant_habitat_code` | TEXT | ESA WorldCover mode class |
| `external_zone_id` | TEXT | String version of WardID |
| `zone_type` | TEXT | Always `'Ward'` |
| `latitude` | FLOAT | Ward centroid latitude |
| `longitude` | FLOAT | Ward centroid longitude |
| `population_density_per_km2` | FLOAT | WorldPop count × 100; NULL if missing |

### `malaria_observation` table

| Column | Type | Notes |
|--------|------|-------|
| `date_recorded` | DATE (PK with zone_id) | Always the 1st of the month |
| `zone_id` | INTEGER (PK with date_recorded) | FK → zone.zone_id |
| `lst_surface_temp_c` | FLOAT | MODIS LST in Celsius |
| `air_temp_c` | FLOAT | ERA5 2m temperature in Celsius |
| `soil_moisture_pct` | FLOAT | ERA5 volumetric soil water |
| `ndvi_vegetation_index` | FLOAT | MODIS NDVI |
| `ndwi_water_index` | FLOAT | MODIS NDWI |
| `agric_percentage` | FLOAT | Fraction cropland from ESA |