"""
test_integration_db.py
-----------------------
Integration tests for the PostgreSQL upsert functions.

The database cursor is fully mocked via MagicMock — no live Postgres
connection is required.  We assert on:
  - the SQL strings passed to execute_batch
  - the row tuples constructed from the DataFrames
  - ON CONFLICT update clause presence
  - return values (row counts)
  - error / rollback paths
"""

import pytest
import sys
import os
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipeline'))
from eo_datapipeline import (
    build_zone_dataframe,
    build_observation_dataframe,
    upsert_zone_rows,
    upsert_observation_rows,
)


# ================================================================
# upsert_zone_rows
# ================================================================

class TestUpsertZoneRows:

    def test_returns_correct_row_count(self, sample_df, mock_db_cursor):
        df_zone = build_zone_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            count = upsert_zone_rows(mock_db_cursor, df_zone)
        assert count == len(df_zone)

    def test_execute_batch_called_once(self, sample_df, mock_db_cursor):
        df_zone = build_zone_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_zone_rows(mock_db_cursor, df_zone)
            mock_eb.assert_called_once()

    def test_sql_contains_insert_into_zone(self, sample_df, mock_db_cursor):
        df_zone = build_zone_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_zone_rows(mock_db_cursor, df_zone)
            sql = mock_eb.call_args[0][1]
            assert 'INSERT INTO zone' in sql

    def test_sql_contains_on_conflict_clause(self, sample_df, mock_db_cursor):
        df_zone = build_zone_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_zone_rows(mock_db_cursor, df_zone)
            sql = mock_eb.call_args[0][1]
            assert 'ON CONFLICT' in sql
            assert 'DO UPDATE SET' in sql

    def test_sql_targets_zone_id_conflict_key(self, sample_df, mock_db_cursor):
        df_zone = build_zone_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_zone_rows(mock_db_cursor, df_zone)
            sql = mock_eb.call_args[0][1]
            assert 'zone_id' in sql

    def test_rows_are_tuples(self, sample_df, mock_db_cursor):
        df_zone = build_zone_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_zone_rows(mock_db_cursor, df_zone)
            rows = mock_eb.call_args[0][2]
            assert all(isinstance(r, tuple) for r in rows)

    def test_row_tuple_has_eleven_fields(self, sample_df, mock_db_cursor):
        """Zone table has 11 columns; each tuple must have exactly 11 values."""
        df_zone = build_zone_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_zone_rows(mock_db_cursor, df_zone)
            rows = mock_eb.call_args[0][2]
            assert all(len(r) == 11 for r in rows)

    def test_first_field_is_zone_id_integer(self, sample_df, mock_db_cursor):
        df_zone = build_zone_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_zone_rows(mock_db_cursor, df_zone)
            rows = mock_eb.call_args[0][2]
            assert isinstance(rows[0][0], (int,))

    def test_cursor_passed_to_execute_batch(self, sample_df, mock_db_cursor):
        df_zone = build_zone_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_zone_rows(mock_db_cursor, df_zone)
            assert mock_eb.call_args[0][0] is mock_db_cursor

    def test_update_includes_population_density(self, sample_df, mock_db_cursor):
        df_zone = build_zone_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_zone_rows(mock_db_cursor, df_zone)
            sql = mock_eb.call_args[0][1]
            assert 'population_density_per_km2' in sql

    def test_zero_rows_does_not_crash(self, sample_df, mock_db_cursor):
        """Empty DataFrame should still call execute_batch with an empty list."""
        import pandas as pd
        df_zone = build_zone_dataframe(sample_df).iloc[0:0]  # empty
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            count = upsert_zone_rows(mock_db_cursor, df_zone)
            assert count == 0
            mock_eb.assert_called_once()


# ================================================================
# upsert_observation_rows
# ================================================================

class TestUpsertObservationRows:

    def test_returns_correct_row_count(self, sample_df, mock_db_cursor):
        df_obs = build_observation_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch'):
            count = upsert_observation_rows(mock_db_cursor, df_obs)
        assert count == len(df_obs)

    def test_execute_batch_called_once(self, sample_df, mock_db_cursor):
        df_obs = build_observation_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_observation_rows(mock_db_cursor, df_obs)
            mock_eb.assert_called_once()

    def test_sql_contains_insert_into_malaria_observation(self, sample_df, mock_db_cursor):
        df_obs = build_observation_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_observation_rows(mock_db_cursor, df_obs)
            sql = mock_eb.call_args[0][1]
            assert 'INSERT INTO malaria_observation' in sql

    def test_sql_contains_on_conflict_clause(self, sample_df, mock_db_cursor):
        df_obs = build_observation_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_observation_rows(mock_db_cursor, df_obs)
            sql = mock_eb.call_args[0][1]
            assert 'ON CONFLICT' in sql
            assert 'DO UPDATE SET' in sql

    def test_conflict_key_is_zone_id_and_date(self, sample_df, mock_db_cursor):
        df_obs = build_observation_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_observation_rows(mock_db_cursor, df_obs)
            sql = mock_eb.call_args[0][1]
            assert 'zone_id' in sql
            assert 'date_recorded' in sql

    def test_rows_are_tuples(self, sample_df, mock_db_cursor):
        df_obs = build_observation_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_observation_rows(mock_db_cursor, df_obs)
            rows = mock_eb.call_args[0][2]
            assert all(isinstance(r, tuple) for r in rows)

    def test_row_tuple_has_eight_fields(self, sample_df, mock_db_cursor):
        """malaria_observation has 8 columns; each tuple must have exactly 8."""
        df_obs = build_observation_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_observation_rows(mock_db_cursor, df_obs)
            rows = mock_eb.call_args[0][2]
            assert all(len(r) == 8 for r in rows)

    def test_page_size_is_1000(self, sample_df, mock_db_cursor):
        """Batch inserts must use page_size=1000 for performance."""
        df_obs = build_observation_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_observation_rows(mock_db_cursor, df_obs)
            kwargs = mock_eb.call_args[1]
            assert kwargs.get('page_size') == 1000

    def test_update_clause_includes_all_env_indicators(self, sample_df, mock_db_cursor):
        df_obs = build_observation_dataframe(sample_df)
        required_cols = [
            'lst_surface_temp_c', 'air_temp_c', 'soil_moisture_pct',
            'ndvi_vegetation_index', 'ndwi_water_index', 'agric_percentage'
        ]
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            upsert_observation_rows(mock_db_cursor, df_obs)
            sql = mock_eb.call_args[0][1]
            for col in required_cols:
                assert col in sql, f"Missing column in UPDATE clause: {col}"

    def test_zero_rows_does_not_crash(self, sample_df, mock_db_cursor):
        import pandas as pd
        df_obs = build_observation_dataframe(sample_df).iloc[0:0]
        with patch('eo_datapipeline.execute_batch') as mock_eb:
            count = upsert_observation_rows(mock_db_cursor, df_obs)
            assert count == 0


# ================================================================
# DB connection error path
# ================================================================

class TestDbErrorHandling:

    def test_execute_batch_exception_propagates(self, sample_df, mock_db_cursor):
        """
        If execute_batch raises, the caller (pipeline loop) should catch it
        and rollback. We verify the exception is not silently swallowed.
        """
        df_zone = build_zone_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch', side_effect=Exception("DB timeout")):
            with pytest.raises(Exception, match="DB timeout"):
                upsert_zone_rows(mock_db_cursor, df_zone)

    def test_obs_execute_batch_exception_propagates(self, sample_df, mock_db_cursor):
        df_obs = build_observation_dataframe(sample_df)
        with patch('eo_datapipeline.execute_batch', side_effect=Exception("Connection reset")):
            with pytest.raises(Exception, match="Connection reset"):
                upsert_observation_rows(mock_db_cursor, df_obs)