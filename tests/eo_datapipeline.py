"""
eo_datapipeline.py  --  testable module extraction
----------------------------------------------------
All business logic is extracted into pure functions so that
tests can import them without triggering GCP / GEE authentication.
The original script's top-level side-effects (ee.Initialize, storage.Client)
are kept behind an explicit `run_pipeline()` entry point.
"""

import os
import time
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from io import StringIO

logger = logging.getLogger(__name__)

EXPORT_COLUMNS = [
    'Month', 'WardID', 'WardLabel', 'Municipali', 'Province',
    'latitude', 'longitude',
    'LST_Surface_Temp_C', 'Air_Temp_C', 'Soil_Moisture',
    'NDWI_Water', 'NDVI_Vegetation_Index',
    'Agric_Percentage', 'Population_Density_Per_KM2',
    'Population_Count', 'Habitat_Class_Code'
]

PROVINCES = [
    "Limpopo", "Gauteng", "Mpumalanga", "North West", "Free State",
    "KwaZulu-Natal", "Eastern Cape", "Western Cape", "Northern Cape"
]


# -------------------------------------------------------
# PURE DATA-TRANSFORM FUNCTIONS  (no GEE / GCS deps)
# -------------------------------------------------------

def build_zone_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms a raw CSV DataFrame (as exported from GEE) into
    a clean DataFrame ready to upsert into the `zone` table.
    """
    zone_id            = pd.to_numeric(df['WardID'], errors='coerce')
    zone_name          = df.get('WardLabel', 'Ward ' + df['WardID'].astype(str)).astype(str)
    municipality_name  = df.get('Municipali', '').astype(str)
    province_name_col  = df.get('Province',   '').astype(str)
    base_population    = pd.to_numeric(df.get('Population_Count'),        errors='coerce').fillna(0).astype(int)
    dominant_habitat   = df.get('Habitat_Class_Code').astype(str)
    external_zone_id   = df['WardID'].astype(str)
    latitude           = pd.to_numeric(df.get('latitude'),  errors='coerce')
    longitude          = pd.to_numeric(df.get('longitude'), errors='coerce')
    population_density = pd.to_numeric(df.get('Population_Density_Per_KM2'), errors='coerce')

    result = pd.DataFrame({
        'zone_id':                    zone_id,
        'zone_name':                  zone_name,
        'municipality_name':          municipality_name,
        'province_name':              province_name_col,
        'base_population':            base_population,
        'dominant_habitat_code':      dominant_habitat,
        'external_zone_id':           external_zone_id,
        'zone_type':                  'Ward',
        'latitude':                   latitude,
        'longitude':                  longitude,
        'population_density_per_km2': population_density,
    })

    result = result.dropna(subset=['zone_id'])
    result = result.drop_duplicates(subset=['zone_id'])
    result['zone_id'] = result['zone_id'].astype(int)
    result = result.where(pd.notnull(result), None)
    return result


def build_observation_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms a raw CSV DataFrame into a clean DataFrame ready to
    upsert into the `malaria_observation` table.
    """
    zone_id = pd.to_numeric(df['WardID'], errors='coerce')

    result = pd.DataFrame({
        'date_recorded':         pd.to_datetime(df['Month'] + '-01'),
        'zone_id':               zone_id,
        'lst_surface_temp_c':    df.get('LST_Surface_Temp_C'),
        'air_temp_c':            df.get('Air_Temp_C'),
        'soil_moisture_pct':     df.get('Soil_Moisture'),
        'ndvi_vegetation_index': df.get('NDVI_Vegetation_Index'),
        'ndwi_water_index':      df.get('NDWI_Water'),
        'agric_percentage':      df.get('Agric_Percentage'),
    })

    result = result.dropna(subset=['zone_id', 'date_recorded'])
    result['zone_id'] = result['zone_id'].astype(int)
    result = result.where(pd.notnull(result), None)
    return result


def upsert_zone_rows(cursor, df_zone: pd.DataFrame) -> int:
    """
    Upserts zone rows into PostgreSQL. Returns the number of rows processed.
    """
    cols = [
        'zone_id', 'zone_name', 'municipality_name', 'province_name',
        'base_population', 'dominant_habitat_code', 'external_zone_id',
        'zone_type', 'latitude', 'longitude', 'population_density_per_km2'
    ]
    rows = [tuple(row) for row in df_zone[cols].to_numpy()]
    execute_batch(cursor, """
        INSERT INTO zone (
            zone_id, zone_name, municipality_name, province_name,
            base_population, dominant_habitat_code, external_zone_id,
            zone_type, latitude, longitude, population_density_per_km2
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (zone_id) DO UPDATE SET
            zone_name                  = EXCLUDED.zone_name,
            municipality_name          = EXCLUDED.municipality_name,
            province_name              = EXCLUDED.province_name,
            base_population            = EXCLUDED.base_population,
            dominant_habitat_code      = EXCLUDED.dominant_habitat_code,
            external_zone_id           = EXCLUDED.external_zone_id,
            latitude                   = EXCLUDED.latitude,
            longitude                  = EXCLUDED.longitude,
            population_density_per_km2 = EXCLUDED.population_density_per_km2
    """, rows)
    return len(rows)


def upsert_observation_rows(cursor, df_obs: pd.DataFrame) -> int:
    """
    Upserts malaria_observation rows into PostgreSQL. Returns the number of rows processed.
    """
    cols = [
        'date_recorded', 'zone_id', 'lst_surface_temp_c', 'air_temp_c',
        'soil_moisture_pct', 'ndvi_vegetation_index', 'ndwi_water_index', 'agric_percentage'
    ]
    rows = [tuple(row) for row in df_obs[cols].to_numpy()]
    execute_batch(cursor, """
        INSERT INTO malaria_observation (
            date_recorded, zone_id, lst_surface_temp_c, air_temp_c,
            soil_moisture_pct, ndvi_vegetation_index, ndwi_water_index, agric_percentage
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (zone_id, date_recorded) DO UPDATE SET
            lst_surface_temp_c    = EXCLUDED.lst_surface_temp_c,
            air_temp_c            = EXCLUDED.air_temp_c,
            soil_moisture_pct     = EXCLUDED.soil_moisture_pct,
            ndvi_vegetation_index = EXCLUDED.ndvi_vegetation_index,
            ndwi_water_index      = EXCLUDED.ndwi_water_index,
            agric_percentage      = EXCLUDED.agric_percentage
    """, rows, page_size=1000)
    return len(rows)


def load_csv_from_gcs(storage_client, bucket_name: str, filename: str) -> pd.DataFrame:
    """
    Downloads a CSV blob from GCS and returns it as a DataFrame.
    Isolated here so tests can mock `storage_client` without touching GCS.
    """
    bucket = storage_client.bucket(bucket_name)
    blob   = bucket.blob(f"pipeline_output/{filename}.csv")
    return pd.read_csv(StringIO(blob.download_as_text()))


def poll_task_until_complete(task, filename: str, poll_interval: int = 30) -> bool:
    """
    Polls an Earth Engine task until it is no longer active.
    Returns True if completed successfully, False on failure.
    Isolated here so tests can inject a mock task.
    """
    logger.info(f"Waiting for task: {filename}")
    while task.active():
        time.sleep(poll_interval)

    state = task.status()['state']
    if state != 'COMPLETED':
        logger.error(f"Task failed: {filename}  State: {state}")
        return False

    logger.info(f"Task complete: {filename}")
    return True