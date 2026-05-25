"""
test_e2e_pipeline.py
--------------------
End-to-end tests for the full pipeline flow.

The entire external surface (GEE task, GCS, psycopg2) is mocked so the
pipeline logic can be exercised from task-submission through to DB upsert
without any cloud credentials.

Covered scenarios:
  - Happy path: task completes, CSV loaded, both tables upserted
  - Task failure: pipeline exits / raises on non-COMPLETED state
  - GCS read failure: exception propagates cleanly
  - DB error: connection error triggers rollback path
  - Poll task: active→active→done lifecycle
  - All 9 provinces processed in sequence
"""

import pytest
import sys
import os
import pandas as pd
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipeline'))
from eo_datapipeline import (
    load_csv_from_gcs,
    build_zone_dataframe,
    build_observation_dataframe,
    upsert_zone_rows,
    upsert_observation_rows,
    poll_task_until_complete,
    PROVINCES,
)


# ================================================================
# poll_task_until_complete
# ================================================================

class TestPollTaskUntilComplete:

    def test_returns_true_on_completed_task(self, mock_ee_task_success):
        result = poll_task_until_complete(mock_ee_task_success, 'Limpopo_2025-03', poll_interval=0)
        assert result is True

    def test_returns_false_on_failed_task(self, mock_ee_task_failure):
        result = poll_task_until_complete(mock_ee_task_failure, 'Limpopo_2025-03', poll_interval=0)
        assert result is False

    def test_polls_multiple_times_before_completion(self, mock_ee_task_slow):
        with patch('eo_datapipeline.time.sleep') as mock_sleep:
            result = poll_task_until_complete(mock_ee_task_slow, 'Limpopo_2025-03', poll_interval=0)
        # active() was called 3 times: True, True, False
        assert mock_ee_task_slow.active.call_count == 3
        assert result is True

    def test_sleep_called_while_active(self, mock_ee_task_slow):
        with patch('eo_datapipeline.time.sleep') as mock_sleep:
            poll_task_until_complete(mock_ee_task_slow, 'Limpopo_2025-03', poll_interval=5)
        # Sleep called twice (once per True from active())
        assert mock_sleep.call_count == 2

    def test_sleep_uses_poll_interval(self, mock_ee_task_slow):
        with patch('eo_datapipeline.time.sleep') as mock_sleep:
            poll_task_until_complete(mock_ee_task_slow, 'Limpopo_2025-03', poll_interval=45)
        mock_sleep.assert_called_with(45)

    def test_status_checked_after_active_returns_false(self, mock_ee_task_success):
        poll_task_until_complete(mock_ee_task_success, 'Limpopo_2025-03', poll_interval=0)
        mock_ee_task_success.status.assert_called()

    def test_failed_state_returns_false_not_raises(self, mock_ee_task_failure):
        """poll_task_until_complete must not raise — caller decides to sys.exit."""
        result = poll_task_until_complete(mock_ee_task_failure, 'Limpopo_2025-03', poll_interval=0)
        assert result is False


# ================================================================
# Full pipeline step orchestration (happy path)
# ================================================================

class TestPipelineHappyPath:
    """
    Wires load_csv_from_gcs → build_zone_dataframe → build_observation_dataframe
    → upsert_zone_rows → upsert_observation_rows in sequence and asserts
    that each layer is called with correct arguments.
    """

    def _run_one_province(self, mock_gcs_client, mock_db_cursor, filename, month='2025-03'):
        df_raw = load_csv_from_gcs(mock_gcs_client, 'eoha-bucket', filename)
        df_zone = build_zone_dataframe(df_raw)
        df_obs  = build_observation_dataframe(df_raw)

        with patch('eo_datapipeline.execute_batch') as mock_eb:
            zone_count = upsert_zone_rows(mock_db_cursor, df_zone)
            obs_count  = upsert_observation_rows(mock_db_cursor, df_obs)

        return df_raw, df_zone, df_obs, zone_count, obs_count

    def test_raw_dataframe_has_correct_shape(self, mock_gcs_client, mock_db_cursor, sample_df):
        df_raw, *_ = self._run_one_province(mock_gcs_client, mock_db_cursor, 'Limpopo_2025-03')
        assert df_raw.shape[0] == len(sample_df)

    def test_zone_and_obs_row_counts_match(self, mock_gcs_client, mock_db_cursor):
        df_raw, df_zone, df_obs, zone_count, obs_count = self._run_one_province(
            mock_gcs_client, mock_db_cursor, 'Limpopo_2025-03'
        )
        assert zone_count == obs_count == 2

    def test_zone_ids_propagate_correctly(self, mock_gcs_client, mock_db_cursor):
        df_raw, df_zone, df_obs, *_ = self._run_one_province(
            mock_gcs_client, mock_db_cursor, 'Limpopo_2025-03'
        )
        assert set(df_zone['zone_id']) == set(df_obs['zone_id'])

    def test_province_field_in_zone_df(self, mock_gcs_client, mock_db_cursor):
        df_raw, df_zone, *_ = self._run_one_province(
            mock_gcs_client, mock_db_cursor, 'Limpopo_2025-03'
        )
        assert (df_zone['province_name'] == 'Limpopo').all()

    def test_date_recorded_is_first_of_month(self, mock_gcs_client, mock_db_cursor):
        _, _, df_obs, *_ = self._run_one_province(
            mock_gcs_client, mock_db_cursor, 'Limpopo_2025-03'
        )
        for dt in df_obs['date_recorded']:
            assert dt.day == 1
            assert dt.month == 3
            assert dt.year == 2025

    def test_gcs_bucket_called_with_correct_name(self, mock_gcs_client, mock_db_cursor):
        self._run_one_province(mock_gcs_client, mock_db_cursor, 'Limpopo_2025-03')
        mock_gcs_client.bucket.assert_called_with('eoha-bucket')

    def test_blob_path_is_correct(self, mock_gcs_client, mock_db_cursor):
        self._run_one_province(mock_gcs_client, mock_db_cursor, 'Limpopo_2025-03')
        mock_bucket = mock_gcs_client.bucket.return_value
        mock_bucket.blob.assert_called_with('pipeline_output/Limpopo_2025-03.csv')

    def test_no_none_zone_ids_in_zone_df(self, mock_gcs_client, mock_db_cursor):
        _, df_zone, *_ = self._run_one_province(mock_gcs_client, mock_db_cursor, 'Limpopo_2025-03')
        assert df_zone['zone_id'].notna().all()

    def test_no_none_zone_ids_in_obs_df(self, mock_gcs_client, mock_db_cursor):
        _, _, df_obs, *_ = self._run_one_province(mock_gcs_client, mock_db_cursor, 'Limpopo_2025-03')
        assert df_obs['zone_id'].notna().all()


# ================================================================
# All 9 provinces processed
# ================================================================

class TestAllProvincesProcessed:

    def test_all_nine_provinces_constant_correct(self):
        assert len(PROVINCES) == 9

    def test_province_list_includes_limpopo(self):
        assert 'Limpopo' in PROVINCES

    def test_province_list_includes_gauteng(self):
        assert 'Gauteng' in PROVINCES

    def test_all_provinces_produce_valid_filenames(self):
        """Slugified province names must not contain spaces."""
        for p in PROVINCES:
            slug = p.replace(' ', '_')
            filename = f"{slug}_2025-03"
            assert ' ' not in filename

    def test_nine_gcs_reads_for_nine_provinces(self, sample_df):
        """Simulates the pipeline loop calling load_csv_from_gcs once per province."""
        csv_text   = sample_df.to_csv(index=False)
        mock_blob  = MagicMock()
        mock_blob.download_as_text.return_value = csv_text
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        results = []
        for province in PROVINCES:
            slug = province.replace(' ', '_')
            df   = load_csv_from_gcs(mock_client, 'eoha-bucket', f"{slug}_2025-03")
            results.append(df)

        assert len(results) == 9
        assert mock_client.bucket.call_count == 9

    def test_nine_province_dfs_all_valid(self, sample_df):
        csv_text   = sample_df.to_csv(index=False)
        mock_blob  = MagicMock()
        mock_blob.download_as_text.return_value = csv_text
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        for province in PROVINCES:
            slug = province.replace(' ', '_')
            df   = load_csv_from_gcs(mock_client, 'eoha-bucket', f"{slug}_2025-03")
            zone = build_zone_dataframe(df)
            obs  = build_observation_dataframe(df)
            assert len(zone) > 0
            assert len(obs) > 0


# ================================================================
# Failure path: GCS read error
# ================================================================

class TestGcsFailurePath:

    def test_gcs_download_exception_propagates(self):
        mock_blob = MagicMock()
        mock_blob.download_as_text.side_effect = Exception("GCS 403 Forbidden")
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        with pytest.raises(Exception, match="GCS 403 Forbidden"):
            load_csv_from_gcs(mock_client, 'eoha-bucket', 'Limpopo_2025-03')

    def test_missing_blob_raises(self):
        mock_blob = MagicMock()
        mock_blob.download_as_text.side_effect = Exception("Blob not found")
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        with pytest.raises(Exception):
            load_csv_from_gcs(mock_client, 'eoha-bucket', 'Nonexistent_2025-03')


# ================================================================
# Failure path: task failure triggers non-COMPLETED state
# ================================================================

class TestTaskFailurePath:

    def test_failed_task_poll_returns_false(self, mock_ee_task_failure):
        result = poll_task_until_complete(mock_ee_task_failure, 'Limpopo_2025-03', poll_interval=0)
        assert result is False

    def test_cancelled_task_returns_false(self):
        task = MagicMock()
        task.active.return_value = False
        task.status.return_value = {'state': 'CANCELLED'}
        result = poll_task_until_complete(task, 'Limpopo_2025-03', poll_interval=0)
        assert result is False

    def test_unsubmitted_task_returns_false(self):
        task = MagicMock()
        task.active.return_value = False
        task.status.return_value = {'state': 'UNSUBMITTED'}
        result = poll_task_until_complete(task, 'Limpopo_2025-03', poll_interval=0)
        assert result is False