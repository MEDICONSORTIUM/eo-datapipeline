"""Database operations limited to the separate NCD schema."""

import json

from psycopg2.extras import execute_batch, execute_values

from .sources import SOURCE_ROWS, VARIABLE_ROWS


def check_database(connection):
    """Verify the migration and the NCD role's minimum permissions."""
    tables = ("data_sources", "environmental_variables", "environmental_observations", "etl_runs")
    sequences = (
        "data_sources_data_source_id_seq",
        "environmental_variables_variable_id_seq",
        "environmental_observations_observation_id_seq",
        "etl_runs_etl_run_id_seq",
    )
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database(), current_user")
        identity = cursor.fetchone()
        if identity[0] != "eoha":
            raise ValueError("Expected the eoha database. Set DB_NAME=eoha.")
        cursor.execute("SELECT region_id, region_code FROM eoha.regions LIMIT 0")
        cursor.execute("SELECT has_table_privilege(current_user, 'eoha.regions', 'SELECT')")
        if not cursor.fetchone()[0]:
            raise ValueError("Database user needs SELECT on eoha.regions.")
        for table in tables:
            cursor.execute(f"SELECT 1 FROM ncd.{table} LIMIT 0")
            cursor.execute(
                "SELECT has_table_privilege(current_user, %s, 'INSERT') "
                "AND has_table_privilege(current_user, %s, 'UPDATE')",
                (f"ncd.{table}", f"ncd.{table}"),
            )
            if not cursor.fetchone()[0]:
                raise ValueError(f"Database user needs INSERT and UPDATE on ncd.{table}.")
        for sequence in sequences:
            cursor.execute(
                "SELECT has_sequence_privilege(current_user, %s, 'USAGE')",
                (f"ncd.{sequence}",),
            )
            if not cursor.fetchone()[0]:
                raise ValueError(f"Database user needs USAGE on ncd.{sequence}.")
    return identity


def ensure_reference_data(cursor):
    """Upsert the approved source and indicator definitions."""
    execute_batch(
        cursor,
        """
        INSERT INTO ncd.data_sources (
            source_code, source_name, provider, dataset_id, source_version,
            source_type, temporal_resolution, spatial_resolution
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_code) DO UPDATE SET
            source_name = EXCLUDED.source_name,
            provider = EXCLUDED.provider,
            dataset_id = EXCLUDED.dataset_id,
            source_version = EXCLUDED.source_version,
            source_type = EXCLUDED.source_type,
            temporal_resolution = EXCLUDED.temporal_resolution,
            spatial_resolution = EXCLUDED.spatial_resolution,
            updated_at = NOW()
        """,
        SOURCE_ROWS,
    )
    execute_batch(
        cursor,
        """
        INSERT INTO ncd.environmental_variables (
            variable_code, variable_name, unit, measurement_type,
            native_time_resolution, definition
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (variable_code) DO UPDATE SET
            variable_name = EXCLUDED.variable_name,
            unit = EXCLUDED.unit,
            measurement_type = EXCLUDED.measurement_type,
            native_time_resolution = EXCLUDED.native_time_resolution,
            definition = EXCLUDED.definition,
            updated_at = NOW()
        """,
        VARIABLE_ROWS,
    )


def start_run(connection, requested_month, province, smoke_test):
    """Create one auditable NCD pipeline run."""
    with connection, connection.cursor() as cursor:
        ensure_reference_data(cursor)
        cursor.execute(
            """
            INSERT INTO ncd.etl_runs (requested_month, province, smoke_test, metadata)
            VALUES (%s, %s, %s, %s::jsonb)
            RETURNING etl_run_id
            """,
            (
                requested_month,
                province,
                smoke_test,
                json.dumps({"pipeline_version": "ncd-v1", "boundary_version": "MDB_Wards_2020"}),
            ),
        )
        return cursor.fetchone()[0]


def load_observations(connection, frame):
    """Resolve shared ward IDs and safely upsert normalized observations."""
    if frame.empty:
        return 0, 0
    columns = [
        "region_code", "variable_code", "source_code", "period_start", "period_end",
        "statistic", "value", "sample_count", "coverage_fraction", "quality_flag",
        "method_version", "boundary_version", "etl_run_id", "processing_metadata",
    ]
    rows = [tuple(row[column] for column in columns) for _, row in frame.iterrows()]
    with connection, connection.cursor() as cursor:
        results = execute_values(
            cursor,
            """
            INSERT INTO ncd.environmental_observations AS existing (
                region_id, variable_id, data_source_id, period_start, period_end,
                statistic, value, sample_count, coverage_fraction, quality_flag,
                method_version, boundary_version, etl_run_id, processing_metadata
            )
            SELECT r.region_id, v.variable_id, s.data_source_id,
                   input.period_start, input.period_end, input.statistic, input.value,
                   input.sample_count, input.coverage_fraction, input.quality_flag,
                   input.method_version, input.boundary_version, input.etl_run_id,
                   input.processing_metadata::jsonb
            FROM (VALUES %s) AS input (
                region_code, variable_code, source_code, period_start, period_end,
                statistic, value, sample_count, coverage_fraction, quality_flag,
                method_version, boundary_version, etl_run_id, processing_metadata
            )
            JOIN eoha.regions r ON r.region_code = input.region_code
            JOIN ncd.environmental_variables v ON v.variable_code = input.variable_code
            JOIN ncd.data_sources s ON s.source_code = input.source_code
            ON CONFLICT (
                region_id, variable_id, data_source_id, period_start, period_end,
                statistic, method_version, boundary_version
            ) DO UPDATE SET
                value = CASE
                    WHEN EXCLUDED.value IS NULL AND existing.value IS NOT NULL
                    THEN existing.value ELSE EXCLUDED.value END,
                sample_count = CASE
                    WHEN EXCLUDED.value IS NULL AND existing.value IS NOT NULL
                    THEN existing.sample_count ELSE EXCLUDED.sample_count END,
                coverage_fraction = CASE
                    WHEN EXCLUDED.value IS NULL AND existing.value IS NOT NULL
                    THEN existing.coverage_fraction ELSE EXCLUDED.coverage_fraction END,
                quality_flag = CASE
                    WHEN EXCLUDED.value IS NULL AND existing.value IS NOT NULL
                    THEN existing.quality_flag ELSE EXCLUDED.quality_flag END,
                etl_run_id = CASE
                    WHEN EXCLUDED.value IS NULL AND existing.value IS NOT NULL
                    THEN existing.etl_run_id ELSE EXCLUDED.etl_run_id END,
                processing_metadata = CASE
                    WHEN EXCLUDED.value IS NULL AND existing.value IS NOT NULL
                    THEN existing.processing_metadata ELSE EXCLUDED.processing_metadata END,
                updated_at = NOW()
            RETURNING (xmax = 0)
            """,
            rows,
            page_size=1000,
            fetch=True,
        )
        if len(results) != len(rows):
            raise ValueError(
                "One or more NCD ward, indicator, or source identifiers could not be resolved."
            )
    inserted = sum(1 for result in results if result[0])
    return inserted, len(results) - inserted


def finish_run(connection, etl_run_id, status, inserted, updated, failed, error=None):
    """Store final counts and error details for one run."""
    with connection, connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE ncd.etl_runs
            SET run_finished_at = NOW(), status = %s, records_inserted = %s,
                records_updated = %s, records_failed = %s, error_message = %s
            WHERE etl_run_id = %s
            """,
            (status, inserted, updated, failed, error, etl_run_id),
        )

