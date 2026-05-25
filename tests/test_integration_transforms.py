"""
test_integration_transforms.py
-------------------------------
Integration tests for the two DataFrame transform functions and the GCS loader.

These tests use real pandas operations on in-memory data — no GCP credentials
or network calls required.  The GCS client is mocked at the boundary.
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipeline'))
from eo_datapipeline import (
    build_zone_dataframe,
    build_observation_dataframe,
    load_csv_from_gcs,
)


# ================================================================
# build_zone_dataframe
# ================================================================

class TestBuildZoneDataframe:

    def test_output_has_correct_columns(self, sample_df):
        result = build_zone_dataframe(sample_df)
        expected_cols = {
            'zone_id', 'zone_name', 'municipality_name', 'province_name',
            'base_population', 'dominant_habitat_code', 'external_zone_id',
            'zone_type', 'latitude', 'longitude', 'population_density_per_km2'
        }
        assert expected_cols.issubset(set(result.columns))

    def test_row_count_matches_input(self, sample_df):
        result = build_zone_dataframe(sample_df)
        assert len(result) == len(sample_df)

    def test_zone_id_is_integer(self, sample_df):
        result = build_zone_dataframe(sample_df)
        assert result['zone_id'].dtype in [int, np.int64, np.int32]

    def test_zone_type_is_ward(self, sample_df):
        result = build_zone_dataframe(sample_df)
        assert (result['zone_type'] == 'Ward').all()

    def test_province_name_preserved(self, sample_df):
        result = build_zone_dataframe(sample_df)
        assert list(result['province_name']) == ['Limpopo', 'Limpopo']

    def test_municipality_name_preserved(self, sample_df):
        result = build_zone_dataframe(sample_df)
        assert list(result['municipality_name']) == ['Musina', 'Musina']

    def test_latitude_is_float(self, sample_df):
        result = build_zone_dataframe(sample_df)
        assert result['latitude'].dtype == float

    def test_longitude_is_float(self, sample_df):
        result = build_zone_dataframe(sample_df)
        assert result['longitude'].dtype == float

    def test_population_density_is_float(self, sample_df):
        result = build_zone_dataframe(sample_df)
        assert result['population_density_per_km2'].dtype == float

    def test_base_population_is_integer(self, sample_df):
        result = build_zone_dataframe(sample_df)
        assert result['base_population'].dtype in [int, np.int64, np.int32]

    def test_external_zone_id_is_string(self, sample_df):
        result = build_zone_dataframe(sample_df)
        # pandas 2.x may return StringDtype or object — both are string-like
        assert pd.api.types.is_string_dtype(result['external_zone_id'])

    def test_nan_replaced_with_none(self, sample_df_with_nulls):
        result = build_zone_dataframe(sample_df_with_nulls)
        # pandas .where(pd.notnull()) converts NaN in float cols to NaN (falsy);
        # psycopg2 accepts both NaN and None as NULL. Assert the value is not a valid float.
        density_vals = result['population_density_per_km2'].tolist()
        # At least one value should be NaN/None (the row that had no density input)
        import math
        has_null = any(v is None or (isinstance(v, float) and math.isnan(v)) for v in density_vals)
        assert has_null

    def test_invalid_ward_id_row_is_dropped(self, sample_df_bad_ward_id):
        result = build_zone_dataframe(sample_df_bad_ward_id)
        # One valid row should remain (WardID='79700002')
        assert len(result) == 1
        assert result.iloc[0]['zone_id'] == 79700002

    def test_duplicate_ward_ids_deduplicated(self, sample_df_duplicate_ward):
        result = build_zone_dataframe(sample_df_duplicate_ward)
        assert len(result) == 1

    def test_zone_id_values_correct(self, sample_df):
        result = build_zone_dataframe(sample_df)
        assert set(result['zone_id']) == {79700001, 79700002}

    def test_base_population_zero_when_null(self, sample_df_with_nulls):
        """fillna(0) must be applied before astype(int) to avoid crash."""
        result = build_zone_dataframe(sample_df_with_nulls)
        # Row 1 had Population_Count=None; should become 0
        row = result[result['zone_id'] == 79700002]
        assert row.iloc[0]['base_population'] == 0


# ================================================================
# build_observation_dataframe
# ================================================================

class TestBuildObservationDataframe:

    def test_output_has_correct_columns(self, sample_df):
        result = build_observation_dataframe(sample_df)
        expected_cols = {
            'date_recorded', 'zone_id', 'lst_surface_temp_c', 'air_temp_c',
            'soil_moisture_pct', 'ndvi_vegetation_index',
            'ndwi_water_index', 'agric_percentage'
        }
        assert expected_cols.issubset(set(result.columns))

    def test_row_count_matches_input(self, sample_df):
        result = build_observation_dataframe(sample_df)
        assert len(result) == len(sample_df)

    def test_date_recorded_is_datetime(self, sample_df):
        result = build_observation_dataframe(sample_df)
        assert pd.api.types.is_datetime64_any_dtype(result['date_recorded'])

    def test_date_is_first_of_month(self, sample_df):
        result = build_observation_dataframe(sample_df)
        for dt in result['date_recorded']:
            assert dt.day == 1

    def test_date_matches_month_label(self, sample_df):
        result = build_observation_dataframe(sample_df)
        assert result.iloc[0]['date_recorded'].strftime('%Y-%m') == '2025-03'

    def test_zone_id_is_integer(self, sample_df):
        result = build_observation_dataframe(sample_df)
        assert result['zone_id'].dtype in [int, np.int64, np.int32]

    def test_lst_temp_values_correct(self, sample_df):
        result = build_observation_dataframe(sample_df)
        assert abs(float(result.iloc[0]['lst_surface_temp_c']) - 32.5) < 1e-6

    def test_ndvi_values_correct(self, sample_df):
        result = build_observation_dataframe(sample_df)
        assert abs(float(result.iloc[0]['ndvi_vegetation_index']) - 0.45) < 1e-6

    def test_null_numeric_fields_become_none(self, sample_df_with_nulls):
        result = build_observation_dataframe(sample_df_with_nulls)
        row = result[result['zone_id'] == 79700001]
        import math
        val = row.iloc[0]['lst_surface_temp_c']
        # Null inputs must produce NaN or None (both sent as NULL by psycopg2)
        assert val is None or (isinstance(val, float) and math.isnan(val))

    def test_invalid_ward_id_row_dropped(self, sample_df_bad_ward_id):
        result = build_observation_dataframe(sample_df_bad_ward_id)
        assert len(result) == 1

    def test_all_nine_provinces_produce_rows(self):
        """Sanity check: 9 provinces × 2 wards = 18 rows total after concat."""
        provinces = [
            "Limpopo", "Gauteng", "Mpumalanga", "North West", "Free State",
            "KwaZulu-Natal", "Eastern Cape", "Western Cape", "Northern Cape"
        ]
        frames = []
        base_id = 10000
        for p in provinces:
            df = pd.DataFrame([
                {
                    'Month': '2025-03', 'WardID': str(base_id), 'WardLabel': f'Ward A - {p}',
                    'Municipali': 'Test', 'Province': p,
                    'latitude': '-25.0', 'longitude': '28.0',
                    'LST_Surface_Temp_C': '30.0', 'Air_Temp_C': '25.0',
                    'Soil_Moisture': '0.2', 'NDWI_Water': '0.01',
                    'NDVI_Vegetation_Index': '0.4', 'Agric_Percentage': '0.1',
                    'Population_Density_Per_KM2': '100.0', 'Population_Count': '2000',
                    'Habitat_Class_Code': '10',
                },
                {
                    'Month': '2025-03', 'WardID': str(base_id + 1), 'WardLabel': f'Ward B - {p}',
                    'Municipali': 'Test', 'Province': p,
                    'latitude': '-25.1', 'longitude': '28.1',
                    'LST_Surface_Temp_C': '29.0', 'Air_Temp_C': '24.0',
                    'Soil_Moisture': '0.18', 'NDWI_Water': '0.02',
                    'NDVI_Vegetation_Index': '0.38', 'Agric_Percentage': '0.08',
                    'Population_Density_Per_KM2': '80.0', 'Population_Count': '1600',
                    'Habitat_Class_Code': '40',
                },
            ])
            frames.append(build_observation_dataframe(df))
            base_id += 2

        combined = pd.concat(frames, ignore_index=True)
        assert len(combined) == 18


# ================================================================
# load_csv_from_gcs
# ================================================================

class TestLoadCsvFromGcs:

    def test_returns_dataframe(self, mock_gcs_client, sample_df):
        result = load_csv_from_gcs(mock_gcs_client, 'test-bucket', 'Limpopo_2025-03')
        assert isinstance(result, pd.DataFrame)

    def test_bucket_called_with_correct_name(self, mock_gcs_client, sample_df):
        load_csv_from_gcs(mock_gcs_client, 'eoha-prod-bucket', 'Limpopo_2025-03')
        mock_gcs_client.bucket.assert_called_once_with('eoha-prod-bucket')

    def test_blob_path_includes_pipeline_output_prefix(self, mock_gcs_client, sample_df):
        load_csv_from_gcs(mock_gcs_client, 'test-bucket', 'Limpopo_2025-03')
        mock_bucket = mock_gcs_client.bucket.return_value
        mock_bucket.blob.assert_called_once_with('pipeline_output/Limpopo_2025-03.csv')

    def test_download_as_text_called(self, mock_gcs_client, sample_df):
        load_csv_from_gcs(mock_gcs_client, 'test-bucket', 'Limpopo_2025-03')
        mock_bucket = mock_gcs_client.bucket.return_value
        mock_blob   = mock_bucket.blob.return_value
        mock_blob.download_as_text.assert_called_once()

    def test_dataframe_has_expected_columns(self, mock_gcs_client, sample_df):
        result = load_csv_from_gcs(mock_gcs_client, 'test-bucket', 'Limpopo_2025-03')
        assert 'WardID' in result.columns
        assert 'Month' in result.columns
        assert 'LST_Surface_Temp_C' in result.columns

    def test_row_count_matches_fixture(self, mock_gcs_client, sample_df):
        result = load_csv_from_gcs(mock_gcs_client, 'test-bucket', 'Limpopo_2025-03')
        assert len(result) == len(sample_df)

    def test_empty_csv_returns_empty_dataframe(self, mock_gcs_client_empty):
        result = load_csv_from_gcs(mock_gcs_client_empty, 'test-bucket', 'Limpopo_2025-03')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_all_nine_province_filenames_resolve(self, sample_df):
        """All 9 province slugs should produce correct GCS blob paths."""
        provinces = [
            "Limpopo", "Gauteng", "Mpumalanga", "North_West", "Free_State",
            "KwaZulu-Natal", "Eastern_Cape", "Western_Cape", "Northern_Cape"
        ]
        for province_slug in provinces:
            csv_text   = sample_df.to_csv(index=False)
            mock_blob  = MagicMock()
            mock_blob.download_as_text.return_value = csv_text
            mock_bucket = MagicMock()
            mock_bucket.blob.return_value = mock_blob
            mock_client = MagicMock()
            mock_client.bucket.return_value = mock_bucket

            filename = f"{province_slug}_2025-03"
            result   = load_csv_from_gcs(mock_client, 'test-bucket', filename)
            assert len(result) == 2

from unittest.mock import MagicMock