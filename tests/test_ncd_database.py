"""Offline tests for NCD upserts and run failure records."""

import unittest
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

from ncd import database


class TestNcdDatabase(unittest.TestCase):
    """NCD writes stay idempotent and auditable."""

    def setUp(self):
        self.connection = MagicMock()
        self.cursor = self.connection.cursor.return_value.__enter__.return_value
        self.frame = pd.DataFrame(
            [
                {
                    "region_code": "29200001",
                    "variable_code": "pm25_mean_ug_m3",
                    "source_code": "CAMS_NRT",
                    "period_start": date(2026, 1, 1),
                    "period_end": date(2026, 2, 1),
                    "statistic": "mean_first_day_forecast",
                    "value": 12.0,
                    "sample_count": 248.0,
                    "coverage_fraction": 1.0,
                    "quality_flag": "PASS",
                    "method_version": "ncd-v1",
                    "boundary_version": "MDB_Wards_2020",
                    "etl_run_id": 4,
                    "processing_metadata": "{}",
                }
            ]
        )

    def test_preflight_accepts_separate_schema_permissions(self):
        """
        test_preflight_accepts_separate_schema_permissions()
        ----------------------------------------------------
        The NCD role needs shared-region reads and NCD-only write privileges.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts the complete permission preflight succeeds.
        """
        self.cursor.fetchone.side_effect = [("eoha", "ncd_runtime")] + [(True,)] * 9
        self.assertEqual(
            database.check_database(self.connection), ("eoha", "ncd_runtime")
        )

    @patch("ncd.database.execute_values", return_value=[(True,)])
    def test_upsert_uses_full_observation_identity(self, execute_values):
        """
        test_upsert_uses_full_observation_identity()
        --------------------------------------------
        Reruns update the matching observation rather than adding duplicates.

        Parameters:
        -----------
        execute_values : MagicMock

        Returns:
        --------
        void - asserts the conflict key and inserted count.
        """
        self.assertEqual(database.load_observations(self.connection, self.frame), (1, 0))
        sql = execute_values.call_args.args[1]
        self.assertIn("ON CONFLICT", sql)
        self.assertIn("method_version, boundary_version", sql)
        self.assertIn("JOIN eoha.regions", sql)
        self.assertIn("EXCLUDED.value IS NULL AND existing.value IS NOT NULL", sql)

    @patch("ncd.database.execute_values", return_value=[])
    def test_unresolved_ward_fails_batch(self, execute_values):
        """
        test_unresolved_ward_fails_batch()
        ----------------------------------
        A ward absent from shared regions causes the transaction to fail.

        Parameters:
        -----------
        execute_values : MagicMock

        Returns:
        --------
        void - asserts an unresolved identifier error.
        """
        with self.assertRaisesRegex(ValueError, "could not be resolved"):
            database.load_observations(self.connection, self.frame)

    def test_failure_status_records_counts_and_error(self):
        """
        test_failure_status_records_counts_and_error()
        ------------------------------------------------
        Failed runs retain partial counts and a useful error message.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts the failure update parameters.
        """
        database.finish_run(self.connection, 9, "FAILED", 2, 3, 1, "export failed")
        parameters = self.cursor.execute.call_args.args[1]
        self.assertEqual(parameters, ("FAILED", 2, 3, 1, "export failed", 9))


if __name__ == "__main__":
    unittest.main()

