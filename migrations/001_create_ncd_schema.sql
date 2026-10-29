-- Review and apply this migration before the first live NCD smoke test.
-- It does not modify malaria observations or duplicate ward records.

BEGIN;

CREATE SCHEMA IF NOT EXISTS ncd;

CREATE TABLE ncd.data_sources (
    data_source_id BIGSERIAL PRIMARY KEY,
    source_code TEXT NOT NULL UNIQUE,
    source_name TEXT NOT NULL,
    provider TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    source_version TEXT NOT NULL,
    source_type TEXT NOT NULL,
    temporal_resolution TEXT NOT NULL,
    spatial_resolution TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE ncd.environmental_variables (
    variable_id BIGSERIAL PRIMARY KEY,
    variable_code TEXT NOT NULL UNIQUE,
    variable_name TEXT NOT NULL,
    unit TEXT NOT NULL,
    measurement_type TEXT NOT NULL,
    native_time_resolution TEXT NOT NULL,
    definition TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE ncd.etl_runs (
    etl_run_id BIGSERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL DEFAULT 'ncd_environmental_pipeline',
    requested_month DATE NOT NULL,
    province TEXT,
    smoke_test BOOLEAN NOT NULL DEFAULT FALSE,
    run_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'RUNNING'
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')),
    records_inserted INTEGER NOT NULL DEFAULT 0,
    records_updated INTEGER NOT NULL DEFAULT 0,
    records_failed INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE ncd.environmental_observations (
    observation_id BIGSERIAL PRIMARY KEY,
    region_id BIGINT NOT NULL REFERENCES eoha.regions(region_id),
    variable_id BIGINT NOT NULL REFERENCES ncd.environmental_variables(variable_id),
    data_source_id BIGINT NOT NULL REFERENCES ncd.data_sources(data_source_id),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    statistic TEXT NOT NULL,
    value DOUBLE PRECISION,
    sample_count DOUBLE PRECISION,
    coverage_fraction DOUBLE PRECISION
        CHECK (coverage_fraction IS NULL OR coverage_fraction BETWEEN 0 AND 1),
    quality_flag TEXT NOT NULL
        CHECK (quality_flag IN (
            'PASS', 'LOW_COVERAGE', 'MISSING', 'NO_SOURCE_DATA',
            'UNKNOWN_COVERAGE', 'OUT_OF_RANGE'
        )),
    method_version TEXT NOT NULL,
    boundary_version TEXT NOT NULL,
    etl_run_id BIGINT NOT NULL REFERENCES ncd.etl_runs(etl_run_id),
    processing_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (period_end > period_start),
    UNIQUE (
        region_id,
        variable_id,
        data_source_id,
        period_start,
        period_end,
        statistic,
        method_version,
        boundary_version
    )
);

CREATE INDEX ncd_observations_region_period_idx
    ON ncd.environmental_observations (region_id, period_start, period_end);

CREATE INDEX ncd_observations_variable_period_idx
    ON ncd.environmental_observations (variable_id, period_start, period_end);

COMMIT;

