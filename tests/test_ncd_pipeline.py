"""Offline tests for the NCD entry point and external-service failures."""

import os
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from ncd import earth_engine
from ncd import pipeline


class TestNcdPipeline(unittest.TestCase):
    """CLI rules and failures remain safe before live collection."""

    def test_month_parser_and_approved_start(self):
        """
        test_month_parser_and_approved_start()
        --------------------------------------
        Month parsing returns a precise half-open range, including December.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts normal and year-boundary month ranges.
        """
        self.assertEqual(
            pipeline.month_range("2026-01"), (date(2026, 1, 1), date(2026, 2, 1))
        )
        self.assertEqual(
            pipeline.month_range("2026-12"), (date(2026, 12, 1), date(2027, 1, 1))
        )

    @patch("ncd.earth_engine.ee.batch.Export.table.toCloudStorage")
    def test_export_prefix_cannot_overlap_malaria(self, to_storage):
        """
        test_export_prefix_cannot_overlap_malaria()
        -------------------------------------------
        Every NCD export is forced under ncd/ before a task can start.

        Parameters:
        -----------
        to_storage : MagicMock

        Returns:
        --------
        void - asserts unsafe paths fail and valid paths start.
        """
        with self.assertRaisesRegex(ValueError, "ncd/"):
            earth_engine.export_to_storage(MagicMock(), "bucket", "pipeline_output/file")
        task = to_storage.return_value
        earth_engine.export_to_storage(MagicMock(), "bucket", "ncd/2026-01/file")
        task.start.assert_called_once()

    @patch("ncd.pipeline.create_google_clients")
    @patch("ncd.pipeline.ncd_database.start_run", return_value=11)
    @patch("ncd.pipeline.ncd_database.check_database")
    @patch("ncd.pipeline.connect_database")
    def test_bucket_failure_is_recorded(
        self, connect_database, check_database, start_run, create_google_clients
    ):
        """
        test_bucket_failure_is_recorded()
        ---------------------------------
        A storage preflight failure marks the run failed and submits no export.

        Parameters:
        -----------
        mocked services : MagicMock

        Returns:
        --------
        void - asserts failure logging through the injected service boundaries.
        """
        connection = connect_database.return_value
        bucket = create_google_clients.return_value.bucket.return_value
        bucket.exists.return_value = False
        with patch.dict(os.environ, {"BUCKET_NAME": "missing"}, clear=True), \
             patch.object(pipeline, "load_dotenv"), \
             patch.object(pipeline.ncd_database, "finish_run") as finish_run, \
             patch.object(pipeline, "build_ward_collection") as build_collection:
            with self.assertRaisesRegex(ValueError, "not found"):
                pipeline.main(["--month", "2026-01", "--smoke-test"])
        finish_run.assert_called_once()
        self.assertEqual(finish_run.call_args.args[2], "FAILED")
        build_collection.assert_not_called()
        self.assertEqual(connection.close.call_count, 2)

    @patch("ncd.pipeline.create_google_clients")
    @patch("ncd.pipeline.ncd_database.load_observations", return_value=(24, 0))
    @patch("ncd.pipeline.ncd_database.start_run", return_value=12)
    @patch("ncd.pipeline.ncd_database.check_database")
    @patch("ncd.pipeline.connect_database")
    def test_database_connections_are_short_lived(
        self,
        connect_database,
        check_database,
        start_run,
        load_observations,
        create_google_clients,
    ):
        """
        test_database_connections_are_short_lived()
        -------------------------------------------
        Export waiting cannot leave one database connection idle long enough to expire.

        Parameters:
        -----------
        mocked services : MagicMock

        Returns:
        --------
        void - asserts start, load, and finish each use a fresh connection.
        """
        connections = [MagicMock(), MagicMock(), MagicMock()]
        connect_database.side_effect = connections
        bucket = create_google_clients.return_value.bucket.return_value
        bucket.exists.return_value = True
        bucket.blob.return_value.download_as_text.return_value = "WardID\n1\n"
        observations = [object()] * 24

        with patch.dict(os.environ, {"BUCKET_NAME": "ncd-bucket"}, clear=True), \
             patch.object(pipeline, "load_dotenv"), \
             patch.object(pipeline, "build_ward_collection"), \
             patch.object(pipeline, "export_to_storage", return_value=MagicMock()), \
             patch.object(pipeline, "wait_for_export"), \
             patch.object(pipeline, "build_observation_frame", return_value=observations), \
             patch.object(pipeline.ncd_database, "finish_run") as finish_run:
            pipeline.main(["--month", "2026-01", "--province", "Limpopo"])

        self.assertEqual(connect_database.call_count, 3)
        for connection in connections:
            connection.close.assert_called_once()
        load_observations.assert_called_once_with(connections[1], observations)
        finish_run.assert_called_once_with(connections[2], 12, "SUCCESS", 24, 0, 0)


if __name__ == "__main__":
    unittest.main()

