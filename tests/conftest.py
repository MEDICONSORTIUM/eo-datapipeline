"""
conftest.py  --  shared fixtures for the EOHA pipeline test suite
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock

SAMPLE_CSV_ROWS = [
    {
        'Month': '2025-03', 'WardID': '79700001', 'WardLabel': 'Ward 1 - Musina',
        'Municipali': 'Musina', 'Province': 'Limpopo',
        'latitude': '-22.344', 'longitude': '30.041',
        'LST_Surface_Temp_C': '32.5', 'Air_Temp_C': '28.1', 'Soil_Moisture': '0.21',
        'NDWI_Water': '0.03', 'NDVI_Vegetation_Index': '0.45', 'Agric_Percentage': '0.12',
        'Population_Density_Per_KM2': '150.0', 'Population_Count': '3800',
        'Habitat_Class_Code': '40',
    },
    {
        'Month': '2025-03', 'WardID': '79700002', 'WardLabel': 'Ward 2 - Musina',
        'Municipali': 'Musina', 'Province': 'Limpopo',
        'latitude': '-22.401', 'longitude': '30.102',
        'LST_Surface_Temp_C': '31.8', 'Air_Temp_C': '27.6', 'Soil_Moisture': '0.19',
        'NDWI_Water': '0.05', 'NDVI_Vegetation_Index': '0.41', 'Agric_Percentage': '0.09',
        'Population_Density_Per_KM2': '200.0', 'Population_Count': '5100',
        'Habitat_Class_Code': '10',
    },
]

@pytest.fixture
def sample_df():
    return pd.DataFrame(SAMPLE_CSV_ROWS)

@pytest.fixture
def sample_df_with_nulls():
    rows = [r.copy() for r in SAMPLE_CSV_ROWS]
    rows[0]['LST_Surface_Temp_C'] = None
    rows[0]['Habitat_Class_Code'] = None
    rows[1]['Population_Count'] = None
    rows[1]['Population_Density_Per_KM2'] = None
    return pd.DataFrame(rows)

@pytest.fixture
def sample_df_bad_ward_id():
    rows = [r.copy() for r in SAMPLE_CSV_ROWS]
    rows[0]['WardID'] = 'INVALID'
    return pd.DataFrame(rows)

@pytest.fixture
def sample_df_duplicate_ward():
    rows = [SAMPLE_CSV_ROWS[0].copy(), SAMPLE_CSV_ROWS[0].copy()]
    rows[1]['LST_Surface_Temp_C'] = '99.9'
    return pd.DataFrame(rows)

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

@pytest.fixture
def mock_gcs_client_empty():
    empty_csv = ("Month,WardID,WardLabel,Municipali,Province,latitude,longitude,"
                 "LST_Surface_Temp_C,Air_Temp_C,Soil_Moisture,NDWI_Water,"
                 "NDVI_Vegetation_Index,Agric_Percentage,Population_Density_Per_KM2,"
                 "Population_Count,Habitat_Class_Code\n")
    mock_blob = MagicMock()
    mock_blob.download_as_text.return_value = empty_csv
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    return mock_client

@pytest.fixture
def mock_db_cursor():
    return MagicMock()

@pytest.fixture
def mock_ee_task_success():
    task = MagicMock()
    task.active.return_value = False
    task.status.return_value = {'state': 'COMPLETED'}
    return task

@pytest.fixture
def mock_ee_task_failure():
    task = MagicMock()
    task.active.return_value = False
    task.status.return_value = {'state': 'FAILED', 'error_message': 'Out of memory'}
    return task

@pytest.fixture
def mock_ee_task_slow():
    task = MagicMock()
    task.active.side_effect = [True, True, False]
    task.status.return_value = {'state': 'COMPLETED'}
    return task