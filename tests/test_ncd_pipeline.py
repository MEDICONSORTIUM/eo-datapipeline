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
        selectors = to_storage.call_args.kwargs["selectors"]
        self.assertTrue({"Municipali", "District", "latitude", "longitude"}.issubset(selectors))


class TestSatelliteMeanAndCount(unittest.TestCase):
    """Satellite coverage uses per-pixel counts on a common ward support."""

    @patch("ncd.earth_engine._mean_and_count")
    def test_expected_samples_are_counted_before_cloud_filter(self, mean_and_count):
        """
        test_expected_samples_are_counted_before_cloud_filter()
        -------------------------------------------------------
        Coverage compares available local samples against the cloud-filtered set.

        Parameters:
        -----------
        mean_and_count : MagicMock

        Returns:
        --------
        void - asserts raw and filtered collections remain separate and zeros count.
        """
        collection = MagicMock()
        filtered, available = MagicMock(), MagicMock()
        mean_and_count.side_effect = [filtered, available]
        earth_engine._satellite_mean_and_count(
            collection, "gas_band", "gas", "gas_expected", earth_engine._quality_mask_no2
        )
        collection.map.assert_called_once_with(earth_engine._quality_mask_no2)
        self.assertEqual(mean_and_count.call_args_list[0].args,
                         (collection.map.return_value, "gas_band", "gas"))
        self.assertEqual(mean_and_count.call_args_list[1].args,
                         (collection, "gas_band", "gas"))
        filtered.select("gas_samples").unmask.assert_called_once_with(0, False)
        available.select("gas_samples").unmask.assert_called_once_with(0, False)
        available.select("gas_samples").unmask.return_value.rename.assert_called_once_with("gas_expected")
        collection.size.assert_not_called()


class TestRequestedPeriods(unittest.TestCase):
    """Month ranges are inclusive, ordered and validated before external work."""

    def test_range_crosses_year_boundary_in_order(self):
        """
        test_range_crosses_year_boundary_in_order()
        -------------------------------------------
        Inclusive ranges preserve every month and half-open period across December.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts ordered month starts and the final exclusive end.
        """
        args = pipeline.parse_args(["--start-month", "2026-11", "--end-month", "2027-01"])
        with patch.object(pipeline, "date", wraps=date) as calendar:
            calendar.today.return_value = date(2027, 2, 1)
            periods = pipeline.requested_periods(args)
        self.assertEqual([start for start, end in periods],
                         [date(2026, 11, 1), date(2026, 12, 1), date(2027, 1, 1)])
        self.assertEqual(periods[-1][1], date(2027, 2, 1))

    def test_invalid_ranges_are_rejected(self):
        """
        test_invalid_ranges_are_rejected()
        -----------------------------------
        Ambiguous, reversed, unapproved and incomplete ranges cannot start work.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts each invalid request fails validation.
        """
        cases = [
            ["--start-month", "2026-01"],
            ["--end-month", "2026-09"],
            ["--month", "2026-01", "--start-month", "2026-01", "--end-month", "2026-02"],
            ["--start-month", "2026-03", "--end-month", "2026-01"],
            ["--start-month", "2025-12", "--end-month", "2026-01"],
            ["--start-month", "2026-01", "--end-month", "2026-10"],
            ["--start-month", "bad-date", "--end-month", "2026-09"],
        ]
        with patch.object(pipeline, "date", wraps=date) as calendar:
            calendar.today.return_value = date(2026, 10, 31)
            for arguments in cases:
                with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                    pipeline.requested_periods(pipeline.parse_args(arguments))


class TestSequentialMonths(unittest.TestCase):
    """A single dispatch completes months in order and stops at the first failure."""

    @patch("ncd.pipeline._run_month")
    def test_range_stops_at_failed_month(self, run_month):
        """
        test_range_stops_at_failed_month()
        -----------------------------------
        A failed February must prevent March from starting after January succeeds.

        Parameters:
        -----------
        run_month : MagicMock

        Returns:
        --------
        void - asserts sequential arguments and immediate failure propagation.
        """
        run_month.side_effect = [None, RuntimeError("February failed")]
        with patch.dict(os.environ, {"BUCKET_NAME": "ncd-bucket"}, clear=True), \
             patch.object(pipeline, "load_dotenv"), \
             patch.object(pipeline, "date", wraps=date) as calendar:
            calendar.today.return_value = date(2026, 10, 31)
            with self.assertRaisesRegex(RuntimeError, "February failed"):
                pipeline.main(["--start-month", "2026-01", "--end-month", "2026-03"])
        self.assertEqual([call.args[1] for call in run_month.call_args_list],
                         [date(2026, 1, 1), date(2026, 2, 1)])

    @patch("ncd.pipeline._run_month")
    def test_range_completes_all_months_with_shared_options(self, run_month):
        """
        test_range_completes_all_months_with_shared_options()
        -----------------------------------------------------
        The chosen province and smoke-test setting apply to every requested month.

        Parameters:
        -----------
        run_month : MagicMock

        Returns:
        --------
        void - asserts all months run in chronological order with consistent options.
        """
        with patch.dict(os.environ, {"BUCKET_NAME": "ncd-bucket"}, clear=True), \
             patch.object(pipeline, "load_dotenv"), \
             patch.object(pipeline, "date", wraps=date) as calendar:
            calendar.today.return_value = date(2026, 10, 31)
            pipeline.main(["--start-month", "2026-01", "--end-month", "2026-03",
                           "--province", "Limpopo", "--smoke-test"])
        self.assertEqual([call.args[1] for call in run_month.call_args_list],
                         [date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1)])
        for call in run_month.call_args_list:
            self.assertEqual(call.args[0].province, "Limpopo")
            self.assertTrue(call.args[0].smoke_test)
            self.assertEqual(call.args[3], "ncd-bucket")


class TestBuildWardCollection(unittest.TestCase):
    """Model extraction retains small wards without changing satellite grids."""

    @patch("ncd.earth_engine.ee")
    @patch("ncd.earth_engine._satellite_mean_and_count")
    @patch("ncd.earth_engine._daily_temperature")
    @patch("ncd.earth_engine._mean_and_count")
    def test_models_use_small_ward_grid(self, mean_and_count, daily, satellite, ee):
        """
        test_models_use_small_ward_grid()
        ---------------------------------
        Coarse model cells must not cause small ward polygons to receive zero weight.

        Parameters:
        -----------
        mocked Earth Engine builders : MagicMock

        Returns:
        --------
        void - asserts the model grid is 1 km while satellite extraction stays 3.5 km.
        """
        earth_engine.build_ward_collection(date(2026, 1, 1), date(2026, 2, 1), "Limpopo")
        era_reduction = mean_and_count.return_value.addBands.return_value.reduceRegions
        cams_reduction = ee.Image.cat.return_value.addBands.return_value.reduceRegions
        self.assertEqual(era_reduction.call_args.kwargs["scale"], 1000)
        self.assertEqual(cams_reduction.call_args.kwargs["scale"], 1000)
        self.assertEqual(satellite.return_value.reduceRegions.call_count, 2)
        for call in satellite.return_value.reduceRegions.call_args_list:
            self.assertEqual(call.kwargs["scale"], 3500)


class TestNcdPipelineFailures(unittest.TestCase):
    """External-service failures retain run records and short-lived connections."""

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

