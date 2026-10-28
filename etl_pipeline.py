"""Monthly Earth Engine pipeline for the EOHA PostgreSQL database."""

import argparse
import json
import logging
import os
import time
from datetime import date
from io import StringIO
from pathlib import Path

import ee
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from psycopg2.extras import execute_batch, execute_values

from database import check_database, connect_database


PROJECT_ID = 'modis-temperature-data'
WARD_ASSET = f'projects/{PROJECT_ID}/assets/MDB_Wards_2020'
PROVINCES = [
    'Limpopo', 'Gauteng', 'Mpumalanga', 'North West', 'Free State',
    'KwaZulu-Natal', 'Eastern Cape', 'Western Cape', 'Northern Cape',
]

SOURCE_ROWS = [
    ('MODIS_LST', 'MODIS Land Surface Temperature', 'NASA',
     'MODIS/061/MOD11A1', 'satellite', 'daily', '1 km'),
    ('MODIS_NDVI', 'MODIS Vegetation Indices', 'NASA',
     'MODIS/061/MOD13Q1', 'satellite', '16 day', '250 m'),
    ('MODIS_REFLECTANCE', 'MODIS Surface Reflectance', 'NASA',
     'MODIS/061/MOD09GA', 'satellite', 'daily', '500 m'),
    ('ERA5_LAND', 'ERA5-Land', 'ECMWF',
     'ECMWF/ERA5_LAND/HOURLY', 'reanalysis', 'hourly', '9 km'),
    ('WORLDCOVER', 'ESA WorldCover', 'ESA',
     'ESA/WorldCover/v100', 'satellite', 'annual', '10 m'),
    ('WORLDPOP', 'WorldPop', 'WorldPop',
     'WorldPop/GP/100m/R2025A/v1/pop', 'population', 'annual', '100 m'),
]

VARIABLE_ROWS = [
    ('lst_c', 'Land Surface Temperature', '°C', 'Thermal'),
    ('air_temp_c', 'Air Temperature', '°C', 'Thermal'),
    ('soil_moisture', 'Soil Moisture', 'm3/m3', 'Moisture'),
    ('ndvi', 'Normalized Difference Vegetation Index', 'index', 'Vegetation'),
    ('ndwi', 'Normalized Difference Water Index', 'index', 'Hydrology'),
    ('agriculture_fraction', 'Agricultural Land Fraction', 'fraction', 'Land Cover'),
    ('population', 'Population', 'persons', 'Population'),
    ('land_cover', 'Land Cover', 'class', 'Land Cover'),
]

MEASUREMENTS = [
    ('LST_Surface_Temp_C', 'lst_c', 'MODIS_LST'),
    ('Air_Temp_C', 'air_temp_c', 'ERA5_LAND'),
    ('Soil_Moisture', 'soil_moisture', 'ERA5_LAND'),
    ('NDVI_Vegetation_Index', 'ndvi', 'MODIS_NDVI'),
    ('NDWI_Water', 'ndwi', 'MODIS_REFLECTANCE'),
    ('Agric_Percentage', 'agriculture_fraction', 'WORLDCOVER'),
    ('Population_Count', 'population', 'WORLDPOP'),
    ('Habitat_Class_Code', 'land_cover', 'WORLDCOVER'),
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_google_clients(project_id):
    """Connect to Earth Engine and Cloud Storage."""
    service_account_json = os.environ.get('GCP_SA_JSON')
    if service_account_json:
        info = json.loads(service_account_json)
        credentials = service_account.Credentials.from_service_account_info(
            info,
            scopes=[
                'https://www.googleapis.com/auth/earthengine',
                'https://www.googleapis.com/auth/cloud-platform',
            ],
        )
        ee.Initialize(credentials=credentials, project=project_id)
    else:
        ee.Initialize(project=project_id)
        credentials = ee.data.get_persistent_credentials()
    return storage.Client(project=project_id, credentials=credentials)


def month_range(month_text=None):
    """Return the first day of a month and the first day of the next month."""
    if month_text:
        start = date.fromisoformat(f'{month_text}-01')
    else:
        today = date.today()
        current = date(today.year, today.month, 1)
        start = date(current.year - 1, 12, 1) if current.month == 1 else date(current.year, current.month - 1, 1)
    end = date(start.year + 1, 1, 1) if start.month == 12 else date(start.year, start.month + 1, 1)
    return start, end


def safe_mean(collection, band, scale, offset, name):
    """Return a scaled monthly mean, or a masked image when no data exists."""
    selected = collection.select(band)
    with_data = selected.mean().multiply(scale).add(offset).rename(name)
    without_data = ee.Image().rename(name).selfMask()
    return ee.Image(ee.Algorithms.If(selected.size().gt(0), with_data, without_data))


def build_ward_data(start, end, province, ward_limit=None):
    """Build monthly environmental values for wards in one province."""
    wards = ee.FeatureCollection(WARD_ASSET).filter(ee.Filter.eq('Province', province))
    if ward_limit:
        wards = wards.limit(ward_limit)

    lst = safe_mean(
        ee.ImageCollection('MODIS/061/MOD11A1').filterDate(str(start), str(end)),
        'LST_Day_1km', 0.02, -273.15, 'LST_Surface_Temp_C',
    )
    ndvi = safe_mean(
        ee.ImageCollection('MODIS/061/MOD13Q1').filterDate(str(start), str(end)),
        'NDVI', 0.0001, 0, 'NDVI_Vegetation_Index',
    )

    reflectance = ee.ImageCollection('MODIS/061/MOD09GA').filterDate(str(start), str(end))
    ndwi_value = reflectance.map(
        lambda image: image.normalizedDifference(['sur_refl_b04', 'sur_refl_b02'])
    ).mean().rename('NDWI_Water')
    ndwi_blank = ee.Image().rename('NDWI_Water').selfMask()
    ndwi = ee.Image(ee.Algorithms.If(reflectance.size().gt(0), ndwi_value, ndwi_blank))

    era5 = ee.ImageCollection('ECMWF/ERA5_LAND/HOURLY').filterDate(str(start), str(end))
    era5_value = (
        era5.select(['temperature_2m', 'volumetric_soil_water_layer_1'])
        .mean()
        .subtract(ee.Image([273.15, 0]))
        .rename(['Air_Temp_C', 'Soil_Moisture'])
    )
    era5_blank = ee.Image.cat([
        ee.Image().rename('Air_Temp_C').selfMask(),
        ee.Image().rename('Soil_Moisture').selfMask(),
    ])
    era5_image = ee.Image(ee.Algorithms.If(era5.size().gt(0), era5_value, era5_blank))

    worldcover = ee.ImageCollection('ESA/WorldCover/v100').first()
    agriculture = worldcover.eq(40).rename('Agric_Percentage')
    habitat = worldcover.select('Map')
    population = (
        ee.ImageCollection('WorldPop/GP/100m/R2025A/v1/pop')
        .filter(ee.Filter.eq('country', 'ZAF'))
        .filter(ee.Filter.eq('year', 2020))
        .mean()
        .select('population')
        .rename('Population_Count')
    )

    values = lst.addBands([ndvi, ndwi, era5_image, agriculture, population])
    reduced = values.reduceRegions(wards, ee.Reducer.mean(), 1000)
    habitat_values = habitat.reduceRegions(wards, ee.Reducer.mode(), 1000)
    joined = ee.Join.saveFirst('habitat').apply(
        reduced,
        habitat_values,
        ee.Filter.equals(leftField='WardID', rightField='WardID'),
    )
    month_text = start.strftime('%Y-%m')

    def add_fields(feature):
        feature = ee.Feature(feature)
        centroid = feature.geometry().centroid()
        habitat_feature = ee.Feature(feature.get('habitat'))
        return feature.set({
            'Month': month_text,
            'Habitat_Class_Code': habitat_feature.get('mode'),
            'latitude': centroid.coordinates().get(1),
            'longitude': centroid.coordinates().get(0),
        })

    return ee.FeatureCollection(joined).map(add_fields)


def export_to_storage(collection, bucket_name, filename):
    """Start one Earth Engine CSV export."""
    selectors = [
        'Month', 'WardID', 'WardLabel', 'Municipali', 'District', 'Province',
        'latitude', 'longitude', 'LST_Surface_Temp_C', 'Air_Temp_C',
        'Soil_Moisture', 'NDWI_Water', 'NDVI_Vegetation_Index',
        'Agric_Percentage', 'Population_Count', 'Habitat_Class_Code',
    ]
    task = ee.batch.Export.table.toCloudStorage(
        collection=collection,
        description=f'Export_{filename}',
        bucket=bucket_name,
        fileNamePrefix=f'pipeline_output/{filename}',
        fileFormat='CSV',
        selectors=selectors,
    )
    task.start()
    return task


def wait_for_export(task, poll_seconds=20):
    """Wait for an Earth Engine export and raise a clear error on failure."""
    while task.active():
        time.sleep(poll_seconds)
    status = task.status()
    if status.get('state') != 'COMPLETED':
        message = status.get('error_message', status)
        raise RuntimeError(f'Earth Engine export failed: {message}')


def build_region_rows(frame):
    """Convert an exported CSV into region rows."""
    rows = []
    for _, item in frame.iterrows():
        ward_id = item.get('WardID')
        if pd.isna(ward_id):
            continue
        code = str(ward_id).removesuffix('.0')
        name = item.get('WardLabel')
        if pd.isna(name) or not str(name).strip():
            name = f'Ward {code}'
        rows.append((
            code, str(name), 'Ward',
            None if pd.isna(item.get('Province')) else str(item.get('Province')),
        ))
    return rows


def build_observation_rows(frame, month_start, etl_run_id):
    """Convert wide Earth Engine output into normalized observation rows."""
    rows = []
    for _, item in frame.iterrows():
        ward_id = item.get('WardID')
        if pd.isna(ward_id):
            continue
        region_code = str(ward_id).removesuffix('.0')
        metadata = json.dumps({
            'municipality': None if pd.isna(item.get('Municipali')) else item.get('Municipali'),
            'district': None if pd.isna(item.get('District')) else item.get('District'),
            'latitude': None if pd.isna(item.get('latitude')) else item.get('latitude'),
            'longitude': None if pd.isna(item.get('longitude')) else item.get('longitude'),
        })
        for column, variable_code, source_code in MEASUREMENTS:
            value = pd.to_numeric(item.get(column), errors='coerce')
            rows.append((
                region_code, variable_code, source_code, month_start,
                None if pd.isna(value) else float(value), etl_run_id, metadata,
            ))
    return rows


def ensure_reference_data(cursor):
    """Add or update the sources and variables used by this pipeline."""
    execute_batch(cursor, '''
        INSERT INTO eoha.data_sources (
            source_code, source_name, provider, dataset_id, source_type,
            temporal_resolution, spatial_resolution
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_code) DO UPDATE SET
            source_name = EXCLUDED.source_name,
            provider = EXCLUDED.provider,
            dataset_id = EXCLUDED.dataset_id,
            source_type = EXCLUDED.source_type,
            temporal_resolution = EXCLUDED.temporal_resolution,
            spatial_resolution = EXCLUDED.spatial_resolution,
            active = TRUE
    ''', SOURCE_ROWS)
    execute_batch(cursor, '''
        INSERT INTO eoha.environmental_variables (
            variable_code, variable_name, unit, category
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT (variable_code) DO UPDATE SET
            variable_name = EXCLUDED.variable_name,
            unit = EXCLUDED.unit,
            category = EXCLUDED.category
    ''', VARIABLE_ROWS)


def start_etl_run(connection, month_start, smoke_test):
    """Create an ETL run record and return its ID."""
    with connection, connection.cursor() as cursor:
        ensure_reference_data(cursor)
        cursor.execute('''
            INSERT INTO eoha.etl_runs (observation_month, metadata)
            VALUES (%s, %s::jsonb)
            RETURNING etl_run_id
        ''', (month_start, json.dumps({'smoke_test': smoke_test})))
        return cursor.fetchone()[0]


def write_frame(cursor, frame, month_start, etl_run_id):
    """Write one exported frame with an existing database cursor."""
    region_rows = build_region_rows(frame)
    observation_rows = build_observation_rows(frame, month_start, etl_run_id)
    execute_batch(cursor, '''
        INSERT INTO eoha.regions (
            region_code, region_name, region_type, province, geometry
        ) VALUES (%s, %s, %s, %s, NULL)
        ON CONFLICT (region_code) DO UPDATE SET
            region_name = EXCLUDED.region_name,
            region_type = EXCLUDED.region_type,
            province = EXCLUDED.province,
            geometry = EXCLUDED.geometry,
            updated_at = NOW()
    ''', region_rows)

    results = execute_values(cursor, '''
        INSERT INTO eoha.environmental_observations (
            region_id, variable_id, data_source_id, observation_month,
            statistic, value, etl_run_id, metadata
        )
        SELECT r.region_id, v.variable_id, s.data_source_id, input.observation_month,
               'mean', input.value, input.etl_run_id, input.metadata::jsonb
        FROM (VALUES %s) AS input (
            region_code, variable_code, source_code, observation_month,
            value, etl_run_id, metadata
        )
        JOIN eoha.regions r ON r.region_code = input.region_code
        JOIN eoha.environmental_variables v ON v.variable_code = input.variable_code
        JOIN eoha.data_sources s ON s.source_code = input.source_code
        ON CONFLICT (region_id, variable_id, data_source_id, observation_month, statistic)
        DO UPDATE SET
            value = EXCLUDED.value,
            etl_run_id = EXCLUDED.etl_run_id,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        RETURNING (xmax = 0)
    ''', observation_rows, page_size=1000, fetch=True)

    inserted = sum(1 for result in results if result[0])
    return inserted, len(results) - inserted


def load_frame(connection, frame, month_start, etl_run_id):
    """Upsert one exported frame and return inserted and updated counts."""
    with connection, connection.cursor() as cursor:
        return write_frame(cursor, frame, month_start, etl_run_id)


def finish_etl_run(connection, etl_run_id, status, inserted, updated, error=None):
    """Store the final status and counts for an ETL run."""
    with connection, connection.cursor() as cursor:
        cursor.execute('''
            UPDATE eoha.etl_runs
            SET run_finished_at = NOW(), status = %s,
                records_inserted = %s, records_updated = %s,
                records_failed = %s, error_message = %s
            WHERE etl_run_id = %s
        ''', (status, inserted, updated, 1 if error else 0, error, etl_run_id))


def parse_args(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--month', help='Month to process, for example 2026-09.')
    parser.add_argument('--province', choices=PROVINCES, help='Process only one province.')
    parser.add_argument('--smoke-test', action='store_true', help='Process three wards only.')
    parser.add_argument('--bucket', help='Cloud Storage bucket; overrides BUCKET_NAME.')
    return parser.parse_args(arguments)


def main(arguments=()):
    args = parse_args(arguments)
    load_dotenv(Path(__file__).with_name('.env'), override=False)
    project_id = os.environ.get('GCP_PROJECT', PROJECT_ID)
    bucket_name = args.bucket or os.environ.get('BUCKET_NAME')
    if not bucket_name:
        raise ValueError('Set BUCKET_NAME or pass --bucket.')

    connection = connect_database()
    etl_run_id = None
    inserted = 0
    updated = 0
    try:
        with connection:
            database_name, username = check_database(connection)
        logging.info('Database ready: %s (user: %s)', database_name, username)

        storage_client = create_google_clients(project_id)
        bucket = storage_client.bucket(bucket_name)
        if not bucket.exists():
            raise ValueError(f'Cloud Storage bucket not found: {bucket_name}')

        start, end = month_range(args.month)
        etl_run_id = start_etl_run(connection, start, args.smoke_test)
        provinces = [args.province] if args.province else PROVINCES
        if args.smoke_test and not args.province:
            provinces = [PROVINCES[0]]

        tasks = []
        for province in provinces:
            slug = province.replace(' ', '_')
            suffix = '_smoke' if args.smoke_test else ''
            filename = f'{slug}_{start:%Y-%m}{suffix}'
            collection = build_ward_data(start, end, province, 3 if args.smoke_test else None)
            task = export_to_storage(collection, bucket_name, filename)
            tasks.append((task, filename))
            logging.info('Export started: %s', filename)

        for task, filename in tasks:
            wait_for_export(task)
            blob = bucket.blob(f'pipeline_output/{filename}.csv')
            frame = pd.read_csv(StringIO(blob.download_as_text()))
            new_rows, changed_rows = load_frame(connection, frame, start, etl_run_id)
            inserted += new_rows
            updated += changed_rows
            logging.info('Loaded %s: %s observations', filename, len(frame) * len(MEASUREMENTS))
            if args.smoke_test:
                blob.delete()

        finish_etl_run(connection, etl_run_id, 'SUCCESS', inserted, updated)
        logging.info('Pipeline complete. Inserted: %s, updated: %s', inserted, updated)
    except Exception as error:
        if etl_run_id is not None:
            finish_etl_run(connection, etl_run_id, 'FAILED', inserted, updated, str(error))
        raise
    finally:
        connection.close()


if __name__ == '__main__':
    try:
        main(None)
    except Exception as error:
        logging.error('Pipeline failed: %s', error)
        raise SystemExit(1)
