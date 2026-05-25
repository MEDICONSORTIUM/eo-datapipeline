# 🧪 EOHA Pipeline – Test Suite Documentation

<div align="center">

![Python](https://img.shields.io/badge/-Python_3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pytest](https://img.shields.io/badge/-Pytest_9.0.3-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white)
![Pandas](https://img.shields.io/badge/-Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![GEE](https://img.shields.io/badge/-Google_Earth_Engine-34A853?style=for-the-badge&logo=google&logoColor=white)

**98 tests · 4 modules · 0 credentials required**

</div>

---

## 📋 Table of Contents

1. [Philosophy](#philosophy)
2. [Architecture Overview](#architecture-overview)
3. [Shared Fixtures — conftest.py](#shared-fixtures)
4. [Module 1 — Unit Tests: GEE Composition](#module-1--unit-tests-gee-composition)
5. [Module 2 — Integration Tests: DataFrame Transforms](#module-2--integration-tests-dataframe-transforms)
6. [Module 3 — Integration Tests: PostgreSQL Upsert](#module-3--integration-tests-postgresql-upsert)
7. [Module 4 — End-to-End Pipeline Tests](#module-4--end-to-end-pipeline-tests)
8. [Running the Tests](#running-the-tests)
9. [Understanding Test Output](#understanding-test-output)
10. [Adding New Tests](#adding-new-tests)

---

## Philosophy

The test suite follows three core principles:

**1. No credentials, ever.**
No GEE project access, no GCS bucket, no live Postgres connection is required to run any test. Every external surface is mocked at the module boundary using Python's `unittest.mock.MagicMock`. Tests that require the real API are left as explicitly documented future work.

**2. Test behaviour, not implementation.**
Tests assert what a function *does* — return types, column presence, SQL structure, API call arguments — not *how* it does it. This keeps tests resilient to refactoring.

**3. Each test has exactly one reason to fail.**
Every test class groups tests by concern (schema, type safety, null handling, edge cases). Within a class each test method is atomic: it sets up minimal state, calls one function, and makes one or two assertions.

---

## Architecture Overview

```
tests/
├── conftest.py                   # Shared fixtures (all modules)
├── test_unit.py                  # GEE composition layer (13 tests)
├── test_integration_transforms.py  # DataFrame transforms + GCS loader (27 tests)
├── test_integration_db.py        # PostgreSQL upsert logic (23 tests)
└── test_e2e_pipeline.py          # Full pipeline flow (35 tests)
```

```
pipeline/
└── eo_datapipeline.py            # Testable module (pure functions, no top-level side-effects)
```

The `etl_pipeline.py` script is the original monolithic pipeline. `eo_datapipeline.py` is a refactored extraction that wraps all business logic in pure functions so that tests can import them without triggering GCP/GEE authentication at import time.

| Module | Layer | Tests | External Mocks |
|--------|-------|-------|---------------|
| `test_unit.py` | GEE composition | 13 | `ee` module (MagicMock) |
| `test_integration_transforms.py` | GCS + DataFrame | 27 | `storage.Client` (MagicMock) |
| `test_integration_db.py` | PostgreSQL upsert | 23 | `psycopg2.execute_batch` (patch) |
| `test_e2e_pipeline.py` | Full pipeline flow | 35 | GEE task + GCS + DB (MagicMock) |
| **Total** | | **98** | |

---

## Shared Fixtures

**File:** `tests/conftest.py`

All fixtures are available to every test module automatically — pytest discovers `conftest.py` by convention. No import required.

---

### `sample_df`

```python
@pytest.fixture
def sample_df():
    return pd.DataFrame(SAMPLE_CSV_ROWS)
```

**Purpose:** The canonical two-row DataFrame that mirrors the CSV format exported by GEE for Limpopo wards (Ward 1 and Ward 2, Musina municipality, March 2025).

**Structure:**

| Column | Example Value | Notes |
|--------|--------------|-------|
| `Month` | `'2025-03'` | YYYY-MM format |
| `WardID` | `'79700001'` | Numeric string |
| `WardLabel` | `'Ward 1 - Musina'` | Human-readable name |
| `Municipali` | `'Musina'` | Truncated municipality name from GEE |
| `Province` | `'Limpopo'` | Full province name |
| `latitude` | `'-22.344'` | String, converted to float by transform |
| `longitude` | `'30.041'` | String, converted to float by transform |
| `LST_Surface_Temp_C` | `'32.5'` | Land surface temperature |
| `Air_Temp_C` | `'28.1'` | ERA5 2m air temperature |
| `Soil_Moisture` | `'0.21'` | ERA5 volumetric soil water |
| `NDWI_Water` | `'0.03'` | Normalised difference water index |
| `NDVI_Vegetation_Index` | `'0.45'` | Normalised difference vegetation index |
| `Agric_Percentage` | `'0.12'` | Fraction cropland |
| `Population_Density_Per_KM2` | `'150.0'` | WorldPop derived |
| `Population_Count` | `'3800'` | WorldPop raw count |
| `Habitat_Class_Code` | `'40'` | ESA WorldCover class |

---

### `sample_df_with_nulls`

```python
@pytest.fixture
def sample_df_with_nulls():
    rows = [r.copy() for r in SAMPLE_CSV_ROWS]
    rows[0]['LST_Surface_Temp_C'] = None
    rows[0]['Habitat_Class_Code'] = None
    rows[1]['Population_Count'] = None
    rows[1]['Population_Density_Per_KM2'] = None
    return pd.DataFrame(rows)
```

**Purpose:** Same two rows but with deliberate `None` values injected into numeric fields. Used to verify that null-handling logic (`fillna(0)`, `where(pd.notnull(...), None)`) works correctly and does not crash on missing satellite data.

---

### `sample_df_bad_ward_id`

```python
@pytest.fixture
def sample_df_bad_ward_id():
    rows = [r.copy() for r in SAMPLE_CSV_ROWS]
    rows[0]['WardID'] = 'INVALID'
    return pd.DataFrame(rows)
```

**Purpose:** Row 0 has a non-numeric `WardID`. Used to verify that `pd.to_numeric(..., errors='coerce')` produces `NaN` for the invalid row, and that the subsequent `dropna(subset=['zone_id'])` call removes it, leaving only one valid row.

---

### `sample_df_duplicate_ward`

```python
@pytest.fixture
def sample_df_duplicate_ward():
    rows = [SAMPLE_CSV_ROWS[0].copy(), SAMPLE_CSV_ROWS[0].copy()]
    rows[1]['LST_Surface_Temp_C'] = '99.9'
    return pd.DataFrame(rows)
```

**Purpose:** Two rows sharing the same `WardID`. Used to verify that `drop_duplicates(subset=['zone_id'])` deduplicates to a single row (keeping the first occurrence).

---

### `mock_gcs_client`

```python
@pytest.fixture
def mock_gcs_client(sample_df):
    csv_text = sample_df.to_csv(index=False)
    mock_blob = MagicMock()
    mock_blob.download_as_text.return_value = csv_text
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    return mock_client
```

**Purpose:** A fully mocked Google Cloud Storage client. Calling `.bucket(name).blob(path).download_as_text()` returns the sample CSV string. Used in any test that exercises `load_csv_from_gcs`.

---

### `mock_gcs_client_empty`

**Purpose:** Same structure as `mock_gcs_client` but `download_as_text()` returns a header-only CSV with no data rows. Used to verify that `load_csv_from_gcs` returns an empty `pd.DataFrame` without crashing.

---

### `mock_db_cursor`

```python
@pytest.fixture
def mock_db_cursor():
    return MagicMock()
```

**Purpose:** A bare `MagicMock` standing in for a `psycopg2` cursor. Passed to `upsert_zone_rows` and `upsert_observation_rows` so tests can assert it was forwarded correctly to `execute_batch`.

---

### `mock_ee_task_success` / `mock_ee_task_failure` / `mock_ee_task_slow`

```python
# Success: immediately done, COMPLETED
@pytest.fixture
def mock_ee_task_success():
    task = MagicMock()
    task.active.return_value = False
    task.status.return_value = {'state': 'COMPLETED'}
    return task

# Failure: immediately done, FAILED
@pytest.fixture
def mock_ee_task_failure():
    task = MagicMock()
    task.active.return_value = False
    task.status.return_value = {'state': 'FAILED', 'error_message': 'Out of memory'}
    return task

# Slow: active for two polls, then done
@pytest.fixture
def mock_ee_task_slow():
    task = MagicMock()
    task.active.side_effect = [True, True, False]
    task.status.return_value = {'state': 'COMPLETED'}
    return task
```

**Purpose:** Mock Earth Engine task objects with controlled lifecycle states. `mock_ee_task_slow` uses `side_effect` as a list so successive calls to `active()` return `True`, `True`, then `False`, simulating a two-poll wait before completion.

---

## Module 1 — Unit Tests: GEE Composition

**File:** `tests/test_unit.py`
**Test count:** 13
**What it tests:** The Google Earth Engine image-composition logic — specifically `build_safe_band_image` (the fallback-safe band stacker) and the province-level `reduceRegions` call — without any network I/O.

**How mocking works:**

```python
def make_ee_mock():
    """
    make_ee_mock()
    --------------
    Constructs a minimal MagicMock namespace that mimics the `ee` module's
    deferred-computation chaining API. Every attribute returns another MagicMock,
    allowing code like:
        ee.ImageCollection('...').filterDate(...).mean().rename(...)
    to chain without raising AttributeError.

    Parameters:
    -----------
    none

    Returns:
    --------
    tuple[MagicMock, MagicMock, MagicMock]
        (ee_mock, image_mock, collection_mock)
        ee_mock        — the top-level ee namespace
        image_mock     — returned by ee.Image() and mean()
        collection_mock — returned by ee.ImageCollection()
    """
```

`ee.Algorithms.If` is configured with `side_effect` so tests can force either the with-data or without-data branch to be returned.

---

### `TestBuildSafeBandImage`

**What it tests:** The `build_safe_band_image` helper, which wraps a band selection in an `ee.Algorithms.If` guard to return a masked empty image when no satellite data exists for the month.

| # | Test | What is asserted |
|---|------|-----------------|
| 1 | `test_returns_with_data_branch_when_collection_nonempty` | Result is not `None` when `ee.Algorithms.If` returns the true branch |
| 2 | `test_select_called_with_correct_band` | `collection.select()` receives the exact band name string passed in |
| 3 | `test_mean_is_called_on_filtered_collection` | `.mean()` is called on the filtered collection, not the original |
| 4 | `test_scale_factor_applied` | `.multiply(scale_factor)` is called on the mean image with the correct numeric value |
| 5 | `test_offset_applied` | `.add(offset)` is called after multiply; negative offsets (e.g. `-273.15`) are forwarded unchanged |
| 6 | `test_output_band_renamed` | `.rename(output_name)` is called with the exact output band string |
| 7 | `test_fallback_branch_uses_selfmask` | When `ee.Algorithms.If` is forced to return the false branch, `image.selfMask()` is invoked |

**Example — testing the fallback path:**

```python
def test_fallback_branch_uses_selfmask(self):
    ee, image, collection = make_ee_mock()
    # Force ee.Algorithms.If to return the WITHOUT-data branch
    ee.Algorithms.If.side_effect = lambda cond, true_val, false_val: false_val
    result = self._run(ee, collection, 'NDVI', 0.0001, 0, 'NDVI_Vegetation_Index')
    image.selfMask.assert_called()
```

---

### `TestProcessProvince`

**What it tests:** The province-level composition logic — ward filtering, region reduction, habitat mode extraction, and the per-feature field enrichment.

| # | Test | What is asserted |
|---|------|-----------------|
| 1 | `test_filter_called_with_province_name` | `ee.Filter.eq('Province', 'Limpopo')` is used to subset the ward FeatureCollection |
| 2 | `test_reduce_regions_uses_mean_reducer` | `reduceRegions()` is called with `reducer=ee.Reducer.mean()` |
| 3 | `test_reduce_regions_scale_is_1000` | The `scale` keyword argument equals `1000` (metres) |
| 4 | `test_habitat_mode_reducer_fires` | A separate `reduceRegions` call uses `ee.Reducer.mode()` for the habitat band |
| 5 | `test_population_density_multiplied_by_100` | `ee.Number(...).multiply(100)` is called to convert WorldPop per-100m values to per-km² |
| 6 | `test_month_label_set_on_features` | `fc.map(lambda f: f.set('Month', label))` is invoked |

---

## Module 2 — Integration Tests: DataFrame Transforms

**File:** `tests/test_integration_transforms.py`
**Test count:** 27
**What it tests:** `build_zone_dataframe`, `build_observation_dataframe`, and `load_csv_from_gcs` — using real pandas operations on in-memory DataFrames. No GCP credentials required.

---

### `TestBuildZoneDataframe` (15 tests)

**What it tests:** That `build_zone_dataframe(df)` produces a well-typed, deduplicated, null-safe DataFrame matching the `zone` table schema.

| Concern | Tests |
|---------|-------|
| Schema | Correct 11-column set is present; `zone_type` is `'Ward'` for every row |
| Type safety | `zone_id` is integer; `latitude`/`longitude` are float; `base_population` is integer; `external_zone_id` is string |
| Null handling | NaN/None inputs produce NaN or None in output (psycopg2 sends both as SQL `NULL`); `Population_Count` null defaults to `0` via `fillna(0)` |
| Data integrity | Province and municipality names pass through unchanged |
| Edge cases | Non-numeric `WardID` row is dropped; duplicate `WardID` rows deduplicated to one |

**Key test — null population fallback:**

```python
def test_base_population_zero_when_null(self, sample_df_with_nulls):
    """
    test_base_population_zero_when_null()
    -------------------------------------
    Verifies that a null Population_Count input is converted to 0
    (not a crash) because fillna(0) must be applied before astype(int).

    Parameters:
    -----------
    sample_df_with_nulls : pd.DataFrame
        Fixture with Population_Count=None on row index 1.

    Returns:
    --------
    void — asserts base_population == 0 for the affected ward.
    """
    result = build_zone_dataframe(sample_df_with_nulls)
    row = result[result['zone_id'] == 79700002]
    assert row.iloc[0]['base_population'] == 0
```

**Key test — bad ward ID is dropped:**

```python
def test_invalid_ward_id_row_is_dropped(self, sample_df_bad_ward_id):
    result = build_zone_dataframe(sample_df_bad_ward_id)
    assert len(result) == 1
    assert result.iloc[0]['zone_id'] == 79700002
```

---

### `TestBuildObservationDataframe` (11 tests)

**What it tests:** That `build_observation_dataframe(df)` produces a correctly typed, date-parsed DataFrame matching the `malaria_observation` table schema.

| Concern | Tests |
|---------|-------|
| Schema | Eight required columns present |
| Date logic | `date_recorded` is `datetime64`; day is always 1; year and month match the `Month` label |
| Value correctness | LST and NDVI values match fixture to six decimal places |
| Null handling | Null LST input produces `NaN` or `None` in output |
| Scale test | Nine-province loop × 2 wards = 18 total rows after concatenation |

**Key test — date is always first of month:**

```python
def test_date_is_first_of_month(self, sample_df):
    """
    test_date_is_first_of_month()
    -----------------------------
    The pipeline stores one observation per ward per month. The date is
    always normalised to the 1st of the month so it serves as a stable
    composite key with zone_id.

    Parameters:
    -----------
    sample_df : pd.DataFrame

    Returns:
    --------
    void — asserts dt.day == 1 for every date_recorded value.
    """
    result = build_observation_dataframe(sample_df)
    for dt in result['date_recorded']:
        assert dt.day == 1
```

---

### `TestLoadCsvFromGcs` (8 tests)

**What it tests:** That `load_csv_from_gcs(client, bucket_name, filename)` correctly constructs the GCS blob path, calls the client API in the right order, and returns the CSV as a `pd.DataFrame`.

| Concern | Tests |
|---------|-------|
| Return type | Returns `pd.DataFrame` |
| GCS API contract | `bucket(name)` and `blob('pipeline_output/{filename}.csv')` called with exact arguments |
| Content | Returned DataFrame has expected columns and row count |
| Empty CSV | Header-only response returns empty DataFrame without crashing |
| All provinces | All 9 slugified province filenames produce valid DataFrames |

**Key test — blob path construction:**

```python
def test_blob_path_includes_pipeline_output_prefix(self, mock_gcs_client, sample_df):
    """
    test_blob_path_includes_pipeline_output_prefix()
    -------------------------------------------------
    Confirms that the GCS object key is always prefixed with
    'pipeline_output/' to match the export path set in etl_pipeline.py.
    A mismatch here would cause a silent 404 in production.

    Parameters:
    -----------
    mock_gcs_client : MagicMock
    sample_df : pd.DataFrame

    Returns:
    --------
    void — asserts blob() was called with 'pipeline_output/Limpopo_2025-03.csv'
    """
    load_csv_from_gcs(mock_gcs_client, 'test-bucket', 'Limpopo_2025-03')
    mock_bucket = mock_gcs_client.bucket.return_value
    mock_bucket.blob.assert_called_once_with('pipeline_output/Limpopo_2025-03.csv')
```

---

## Module 3 — Integration Tests: PostgreSQL Upsert

**File:** `tests/test_integration_db.py`
**Test count:** 23
**What it tests:** That `upsert_zone_rows` and `upsert_observation_rows` construct correct SQL, correct row tuples, and correct `execute_batch` arguments — all verified by patching `execute_batch` at the module level.

**How patching works:**

```python
with patch('eo_datapipeline.execute_batch') as mock_eb:
    upsert_zone_rows(mock_db_cursor, df_zone)
    sql  = mock_eb.call_args[0][1]   # Second positional arg = SQL string
    rows = mock_eb.call_args[0][2]   # Third positional arg = list of tuples
```

---

### `TestUpsertZoneRows` (10 tests)

| Concern | Tests |
|---------|-------|
| SQL correctness | `INSERT INTO zone` present; `ON CONFLICT (zone_id) DO UPDATE SET` present; `population_density_per_km2` in `UPDATE` clause |
| Row structure | Each row is a `tuple`; every tuple has exactly **11** fields; first field is `int` |
| API contract | `execute_batch` called exactly once; cursor passed as first argument |
| Return value | Row count equals `len(df_zone)` |
| Edge case | Empty DataFrame returns count `0` without error |

**Key test — 11 fields per tuple:**

```python
def test_row_tuple_has_eleven_fields(self, sample_df, mock_db_cursor):
    """
    test_row_tuple_has_eleven_fields()
    ----------------------------------
    The zone table has 11 columns. If a tuple has fewer or more fields,
    psycopg2 will raise a ProgrammingError at runtime. This test catches
    any schema drift before it reaches production.

    Parameters:
    -----------
    sample_df : pd.DataFrame
    mock_db_cursor : MagicMock

    Returns:
    --------
    void — asserts len(row) == 11 for every row tuple.
    """
    df_zone = build_zone_dataframe(sample_df)
    with patch('eo_datapipeline.execute_batch') as mock_eb:
        upsert_zone_rows(mock_db_cursor, df_zone)
        rows = mock_eb.call_args[0][2]
        assert all(len(r) == 11 for r in rows)
```

---

### `TestUpsertObservationRows` (10 tests)

| Concern | Tests |
|---------|-------|
| SQL correctness | `INSERT INTO malaria_observation`; conflict key is `(zone_id, date_recorded)`; all six environmental columns in `UPDATE` clause |
| Row structure | Each row is a `tuple` with exactly **8** fields |
| Performance | `page_size=1000` keyword argument passed to `execute_batch` |
| Edge case | Empty DataFrame returns `0` without error |

---

### `TestDbErrorHandling` (2 tests)

**What it tests:** That exceptions raised inside `execute_batch` propagate upward to the caller (the pipeline loop), which is responsible for calling `connection.rollback()` and `sys.exit(1)`. Tests verify that the upsert functions do *not* silently swallow exceptions.

```python
def test_execute_batch_exception_propagates(self, sample_df, mock_db_cursor):
    """
    test_execute_batch_exception_propagates()
    -----------------------------------------
    The upsert functions must not swallow database errors. If execute_batch
    raises, the exception must propagate to the caller so it can roll back
    the transaction and exit cleanly.

    Parameters:
    -----------
    sample_df : pd.DataFrame
    mock_db_cursor : MagicMock

    Returns:
    --------
    void — asserts pytest.raises(Exception, match='DB timeout').
    """
    df_zone = build_zone_dataframe(sample_df)
    with patch('eo_datapipeline.execute_batch', side_effect=Exception("DB timeout")):
        with pytest.raises(Exception, match="DB timeout"):
            upsert_zone_rows(mock_db_cursor, df_zone)
```

---

## Module 4 — End-to-End Pipeline Tests

**File:** `tests/test_e2e_pipeline.py`
**Test count:** 35
**What it tests:** The full pipeline chain — task polling → GCS load → DataFrame transform → DB upsert — with every external surface mocked.

---

### `TestPollTaskUntilComplete` (7 tests)

**What it tests:** The `poll_task_until_complete(task, filename, poll_interval)` function, which loops on `task.active()` and returns `True`/`False` based on the final task state.

| Scenario | Expected outcome |
|----------|----------------|
| Task completes immediately | Returns `True` |
| Task fails | Returns `False` (no exception raised — caller decides to `sys.exit`) |
| Active for 2 polls then done | `active()` called 3 times; `sleep()` called twice |
| Poll interval respected | `time.sleep()` called with the configured interval value |
| Status checked after inactive | `task.status()` invoked after `active()` returns `False` |
| `CANCELLED` state | Returns `False` |
| `UNSUBMITTED` state | Returns `False` |

**Key test — sleep uses configured interval:**

```python
def test_sleep_uses_poll_interval(self, mock_ee_task_slow):
    """
    test_sleep_uses_poll_interval()
    --------------------------------
    Verifies that poll_task_until_complete forwards the poll_interval
    argument directly to time.sleep. The default is 30 seconds in
    production; tests override it to 0 to avoid slowing the suite.

    Parameters:
    -----------
    mock_ee_task_slow : MagicMock
        Task that is active for two polls before completing.

    Returns:
    --------
    void — asserts mock_sleep.assert_called_with(45).
    """
    with patch('eo_datapipeline.time.sleep') as mock_sleep:
        poll_task_until_complete(mock_ee_task_slow, 'Limpopo_2025-03', poll_interval=45)
    mock_sleep.assert_called_with(45)
```

---

### `TestPipelineHappyPath` (9 tests)

**What it tests:** The full happy-path chain for a single province: GCS read → zone transform → observation transform → upsert, verifying that data flows correctly through every layer.

| Test | What is verified |
|------|----------------|
| `test_raw_dataframe_has_correct_shape` | GCS mock returns the expected row count |
| `test_zone_and_obs_row_counts_match` | Both upsert functions process the same number of rows |
| `test_zone_ids_propagate_correctly` | `zone_id` set is identical in `df_zone` and `df_obs` |
| `test_province_field_in_zone_df` | Province name is preserved end-to-end |
| `test_date_recorded_is_first_of_month` | Date is `2025-03-01` — correct year, month, and day |
| `test_gcs_bucket_called_with_correct_name` | Bucket name is forwarded exactly |
| `test_blob_path_is_correct` | Full blob path matches the `pipeline_output/` convention |
| `test_no_none_zone_ids_in_zone_df` | No null zone IDs after transform |
| `test_no_none_zone_ids_in_obs_df` | No null zone IDs in observation output |

---

### `TestAllProvincesProcessed` (6 tests)

**What it tests:** That the `PROVINCES` constant is correct and that the pipeline loop correctly processes all nine provinces.

| Test | What is verified |
|------|----------------|
| `test_all_nine_provinces_constant_correct` | `len(PROVINCES) == 9` |
| `test_province_list_includes_limpopo` | `'Limpopo'` is in the list |
| `test_province_list_includes_gauteng` | `'Gauteng'` is in the list |
| `test_all_provinces_produce_valid_filenames` | Slugified names contain no spaces |
| `test_nine_gcs_reads_for_nine_provinces` | `mock_client.bucket` called exactly 9 times |
| `test_nine_province_dfs_all_valid` | Every province produces non-empty zone and observation DataFrames |

---

### `TestGcsFailurePath` (2 tests)

**What it tests:** That GCS errors propagate upward cleanly — a `403 Forbidden` or missing blob exception from `download_as_text()` is not swallowed.

---

### `TestTaskFailurePath` (3 tests)

**What it tests:** That `poll_task_until_complete` returns `False` for all terminal non-success states: `FAILED`, `CANCELLED`, and `UNSUBMITTED`.

---

## Running the Tests

### Prerequisites

```bash
pip install pytest pandas psycopg2-binary
```

> No GCP credentials, GEE project access, or live database connection required.

### Run the Full Suite

```bash
cd eoha_tests
pytest test_unit.py \
       test_integration_transforms.py \
       test_integration_db.py \
       test_e2e_pipeline.py \
       -v --tb=short
```

### Run a Single Module

```bash
# Unit tests only (GEE composition)
pytest test_unit.py -v

# Integration tests only (DataFrame transforms + GCS)
pytest test_integration_transforms.py -v

# Integration tests only (PostgreSQL upsert)
pytest test_integration_db.py -v

# End-to-end tests only
pytest test_e2e_pipeline.py -v
```

### Run a Single Test Class

```bash
pytest test_integration_transforms.py::TestBuildZoneDataframe -v
```

### Run a Single Test

```bash
pytest test_integration_db.py::TestUpsertZoneRows::test_row_tuple_has_eleven_fields -v
```

### Run Only Null-Handling Tests

```bash
pytest -k "null" -v
```

---

## Understanding Test Output

### Passing Run

```
============================= test session starts ==============================
collected 98 items

test_unit.py::TestBuildSafeBandImage::test_returns_with_data_branch_when_collection_nonempty PASSED
test_unit.py::TestBuildSafeBandImage::test_select_called_with_correct_band PASSED
...
========================= 98 passed in 0.82s ==========================
```

### Common Failure Patterns

**`AssertionError: assert 10 == 11`** in `test_row_tuple_has_eleven_fields`
→ A column was added or removed from the zone upsert query. Update `upsert_zone_rows` and this test together.

**`AssertionError: assert 'NaN' is None`** in null-handling tests
→ A `where(pd.notnull(), None)` call is missing. Ensure all output DataFrames have this applied before being returned.

**`AssertionError: call not found`** in GCS path tests
→ The blob path prefix changed. Check `load_csv_from_gcs` and `test_blob_path_includes_pipeline_output_prefix` are in sync.

---

## Adding New Tests

### Checklist

- [ ] Add the new fixture to `conftest.py` if it will be shared across modules
- [ ] Group related tests in a `class` named `TestFeatureName`
- [ ] Add the docstring block to every test function (see format below)
- [ ] Each test method makes **one logical assertion**; use multiple `assert` statements only for compound checks (e.g. row count AND column presence)
- [ ] Never import `ee`, `google.cloud.storage`, or `psycopg2` directly in tests — always mock them
- [ ] Set `poll_interval=0` whenever calling `poll_task_until_complete` in tests

### Docstring Format

Every test function follows this pattern:

```python
def test_my_new_behaviour(self, sample_df, mock_db_cursor):
    """
    test_my_new_behaviour()
    -----------------------
    One sentence describing what behaviour is being verified and why
    it matters for correctness or data integrity.

    Parameters:
    -----------
    sample_df : pd.DataFrame
        The canonical two-row test fixture.
    mock_db_cursor : MagicMock
        Mocked psycopg2 cursor.

    Returns:
    --------
    void — asserts <specific thing being checked>.
    """
    result = function_under_test(sample_df)
    assert result['column'].dtype == float
```