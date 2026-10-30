"""Offline tests for NCD validation, units, missing data, and annual handling."""

import json
import unittest
from datetime import date

import pandas as pd

from ncd.sources import KG_M3_TO_UG_M3, UV_DOSE_RATE_TO_INDEX, VARIABLE_ROWS
from ncd.transform import build_observation_frame, validate_export


class TestNcdTransform(unittest.TestCase):
    """Wide Earth Engine exports become quality-labelled normalized rows."""

    def setUp(self):
        self.frame = pd.DataFrame(
            [
                {
                    "WardID": 29200001,
                    "period_start": "2026-01-01",
                    "period_end": "2026-02-01",
                    "ward_area_m2": 2_000_000,
                    "air_temp_mean_c": 23.5,
                    "air_temp_mean_c_samples": 744,
                    "air_temp_daily_max_mean_c": 30.5,
                    "air_temp_daily_max_mean_c_samples": 31,
                    "era5_expected_samples": 744,
                    "era5_expected_days": 31,
                    "no2_tropospheric_column_mol_m2": 0.0001,
                    "no2_tropospheric_column_mol_m2_samples": 12,
                    "no2_expected_samples": 20,
                    "so2_column_mol_m2": 0.00001,
                    "so2_column_mol_m2_samples": 10,
                    "so2_expected_samples": 20,
                    "pm25_mean_ug_m3": 14.0,
                    "pm25_mean_ug_m3_samples": 248,
                    "pm10_mean_ug_m3": 25.0,
                    "pm10_mean_ug_m3_samples": 248,
                    "pm25_10_mean_ug_m3": 11.0,
                    "pm25_10_mean_ug_m3_samples": 248,
                    "cams_pm_expected_samples": 248,
                    "uv_index_14h_mean": 9.0,
                    "uv_index_14h_mean_samples": 31,
                    "cams_uv_expected_samples": 31,
                    "built_up_fraction": 0.25,
                    "built_up_fraction_samples": 40,
                    "dynamic_world_expected_samples": 100,
                }
            ]
        )

    def test_monthly_rows_keep_values_and_period(self):
        """
        test_monthly_rows_keep_values_and_period()
        ------------------------------------------
        Valid source values retain their approved half-open monthly period.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts eight monthly rows and their quality state.
        """
        result = build_observation_frame(
            self.frame, date(2026, 1, 1), date(2026, 2, 1), 7
        )
        self.assertEqual(len(result), 8)
        self.assertTrue(all(result["quality_flag"] == "PASS"))
        self.assertEqual(set(result["period_start"]), {date(2026, 1, 1)})
        self.assertEqual(set(result["period_end"]), {date(2026, 2, 1)})
        model = result[result["source_code"].isin(["ERA5_LAND", "CAMS_NRT"])]
        for text in model["processing_metadata"]:
            metadata = json.loads(text)
            self.assertEqual(metadata["aggregation_scale_m"], 1000)
            self.assertEqual(metadata["aggregation_method"], "ward_mean_nearest_neighbour_grid")

    def test_missing_value_is_sql_null_not_zero(self):
        """
        test_missing_value_is_sql_null_not_zero()
        -----------------------------------------
        A missing source value must remain None and carry a MISSING flag.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts null preservation and no zero substitution.
        """
        self.frame.loc[0, "pm25_mean_ug_m3"] = None
        result = build_observation_frame(
            self.frame, date(2026, 1, 1), date(2026, 2, 1), 7
        )
        row = result[result["variable_code"] == "pm25_mean_ug_m3"].iloc[0]
        self.assertIsNone(row["value"])
        self.assertEqual(row["quality_flag"], "MISSING")

    def test_satellite_coverage_uses_local_sample_opportunities(self):
        """
        test_satellite_coverage_uses_local_sample_opportunities()
        --------------------------------------------------------
        Local counts retain usable gas values without lowering the 20% gate.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts local coverage, rejection and expected-count provenance.
        """
        for expected, flag in [(20, "PASS"), (300, "LOW_COVERAGE"), (0, "NO_SOURCE_DATA")]:
            with self.subTest(expected=expected):
                self.frame.loc[0, "no2_expected_samples"] = expected
                result = build_observation_frame(
                    self.frame, date(2026, 1, 1), date(2026, 2, 1), 7
                )
                row = result[result["variable_code"] == "no2_tropospheric_column_mol_m2"].iloc[0]
                self.assertEqual(row["quality_flag"], flag)
                if flag == "PASS":
                    self.assertAlmostEqual(row["value"], 0.0001)
                    self.assertAlmostEqual(row["coverage_fraction"], 0.6)
                else:
                    self.assertIsNone(row["value"])
                self.assertEqual(json.loads(row["processing_metadata"])["expected_sample_count"], expected)

    def test_location_metadata_survives_transformation(self):
        """
        test_location_metadata_survives_transformation()
        ------------------------------------------------
        Ward location metadata remains available even for rejected measurements.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts source metadata is retained in every normalized row.
        """
        self.frame["Municipali"] = "Greater Giyani Local Municipality"
        self.frame["District"] = "Mopani"
        self.frame["latitude"] = -23.36990468117883
        self.frame["longitude"] = 30.35782285666032
        self.frame.loc[0, "no2_expected_samples"] = 300
        result = build_observation_frame(
            self.frame, date(2026, 1, 1), date(2026, 2, 1), 7
        )
        for text in result["processing_metadata"]:
            metadata = json.loads(text)
            self.assertEqual(metadata["municipality"], "Greater Giyani Local Municipality")
            self.assertEqual(metadata["district"], "Mopani")
            self.assertAlmostEqual(metadata["latitude"], -23.36990468117883)
            self.assertAlmostEqual(metadata["longitude"], 30.35782285666032)

    def test_missing_location_is_json_null(self):
        """
        test_missing_location_is_json_null()
        ------------------------------------
        Missing export metadata must not become NaN or fabricated coordinates.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts JSON nulls for unavailable location fields.
        """
        self.frame["Municipali"] = float("nan")
        self.frame["latitude"] = "invalid"
        result = build_observation_frame(
            self.frame, date(2026, 1, 1), date(2026, 2, 1), 7
        )
        metadata = json.loads(result.iloc[0]["processing_metadata"])
        for key in ("municipality", "district", "latitude", "longitude"):
            self.assertIsNone(metadata[key])

    def test_low_coverage_value_is_rejected(self):
        """
        test_low_coverage_value_is_rejected()
        -------------------------------------
        Values below their source threshold are stored as null with a clear flag.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts the coverage gate is enforced.
        """
        self.frame.loc[0, "pm10_mean_ug_m3_samples"] = 10
        result = build_observation_frame(
            self.frame, date(2026, 1, 1), date(2026, 2, 1), 7
        )
        row = result[result["variable_code"] == "pm10_mean_ug_m3"].iloc[0]
        self.assertIsNone(row["value"])
        self.assertEqual(row["quality_flag"], "LOW_COVERAGE")
        self.assertAlmostEqual(row["coverage_fraction"], 10 / 248)

    def test_negative_derived_pm_is_rejected(self):
        """
        test_negative_derived_pm_is_rejected()
        --------------------------------------
        A physically invalid negative coarse-particle estimate is not published.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts the source-specific range gate and null value.
        """
        self.frame.loc[0, "pm25_10_mean_ug_m3"] = -2
        result = build_observation_frame(
            self.frame, date(2026, 1, 1), date(2026, 2, 1), 7
        )
        row = result[result["variable_code"] == "pm25_10_mean_ug_m3"].iloc[0]
        self.assertIsNone(row["value"])
        self.assertEqual(row["quality_flag"], "OUT_OF_RANGE")

    def test_annual_values_keep_annual_resolution(self):
        """
        test_annual_values_keep_annual_resolution()
        -------------------------------------------
        December processing adds annual values for the full calendar year only.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts annual periods and derived built area.
        """
        self.frame.loc[0, "period_start"] = "2026-12-01"
        self.frame.loc[0, "period_end"] = "2027-01-01"
        result = build_observation_frame(
            self.frame,
            date(2026, 12, 1),
            date(2027, 1, 1),
            8,
            include_annual=True,
        )
        annual = result[result["variable_code"].str.startswith("built_up")]
        self.assertEqual(len(annual), 2)
        self.assertEqual(set(annual["period_start"]), {date(2026, 1, 1)})
        self.assertEqual(set(annual["period_end"]), {date(2027, 1, 1)})
        area = annual[annual["variable_code"] == "built_up_area_m2"].iloc[0]
        self.assertEqual(area["value"], 500_000)

    def test_unit_constants_and_references(self):
        """
        test_unit_constants_and_references()
        ------------------------------------
        CAMS mass and UV conversions and database unit references stay explicit.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts approved conversion factors and units.
        """
        self.assertEqual(2e-8 * KG_M3_TO_UG_M3, 20.0)
        self.assertEqual(0.20 * UV_DOSE_RATE_TO_INDEX, 8.0)
        units = {row[0]: row[2] for row in VARIABLE_ROWS}
        self.assertEqual(units["pm25_mean_ug_m3"], "ug/m3")
        self.assertEqual(units["no2_tropospheric_column_mol_m2"], "mol/m2")

    def test_duplicate_wards_fail_validation(self):
        """
        test_duplicate_wards_fail_validation()
        --------------------------------------
        A duplicate ward export must fail before database loading.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts duplicate prevention at the validation boundary.
        """
        duplicate = pd.concat([self.frame, self.frame], ignore_index=True)
        with self.assertRaisesRegex(ValueError, "duplicate WardID"):
            validate_export(duplicate)

    def test_wrong_export_period_is_rejected(self):
        """
        test_wrong_export_period_is_rejected()
        --------------------------------------
        A stale staged file cannot be loaded into a different requested month.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts exact period validation before transformation.
        """
        self.frame.loc[0, "period_start"] = "2026-02-01"
        with self.assertRaisesRegex(ValueError, "period"):
            build_observation_frame(
                self.frame, date(2026, 1, 1), date(2026, 2, 1), 7
            )


if __name__ == "__main__":
    unittest.main()

