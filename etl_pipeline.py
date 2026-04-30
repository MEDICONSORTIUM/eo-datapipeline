import ee
import os
import sys
import json
import time
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from io import StringIO
from google.cloud import storage
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# -------------------------------------------------------
# STEP 0: AUTHENTICATE WITH GOOGLE CLOUD AND EARTH ENGINE
# -------------------------------------------------------

service_account_info = json.loads(os.environ['GCP_SA_JSON'])

credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=[
        'https://www.googleapis.com/auth/earthengine',
        'https://www.googleapis.com/auth/cloud-platform'
    ]
)

ee.Initialize(credentials=credentials, project='modis-temperature-data')

storage_client = storage.Client(credentials=credentials)

BUCKET_NAME = os.environ['BUCKET_NAME']


# -------------------------------------------------------
# STEP 1: WORK OUT WHICH MONTH WE ARE PROCESSING
# -------------------------------------------------------

# Get the first day of the current month as the end date,
# and the first day of the previous month as the start date.
now        = ee.Date(time.time() * 1000)
end_date   = ee.Date(now.format("YYYY-MM-01"))
start_date = end_date.advance(-1, 'month')

# month_label will be something like "2025-03", used in filenames and the database.
month_label = start_date.format('YYYY-MM').getInfo()

logging.info(f"Running pipeline for month: {month_label}")


# -------------------------------------------------------
# STEP 2: LOAD SHARED EARTH ENGINE ASSETS
# -------------------------------------------------------

# South African ward boundaries (used as the analysis units).
wards = ee.FeatureCollection("projects/modis-temperature-data/assets/MDB_Wards_2020")

provinces = [
    "Limpopo", "Gauteng", "Mpumalanga", "North West", "Free State",
    "KwaZulu-Natal", "Eastern Cape", "Western Cape", "Northern Cape"
]

# Population count raster (WorldPop 2020).
population_image = (
    ee.ImageCollection("WorldPop/GP/100m/pop")
    .filterDate('2020-01-01', '2021-01-01')
    .mean()
    .rename('Population_Count')
)

# Land cover classification (ESA WorldCover).
landcover_image = ee.ImageCollection("ESA/WorldCover/v100").first()

# A binary image: 1 where land cover is cropland (class 40), 0 elsewhere.
cropland_image = landcover_image.eq(40).rename('Agric_Percentage')

# The raw habitat class codes (used to find the most common habitat per ward).
habitat_image = landcover_image.select('Map').rename('Habitat_Code')


# -------------------------------------------------------
# STEP 3: DEFINE HOW TO BUILD SATELLITE IMAGES SAFELY
# -------------------------------------------------------

def build_safe_band_image(image_collection, band_name, scale_factor, offset, output_name):
    """
    Returns a single image for the given band by averaging the collection.
    Applies a scale factor and offset to convert raw values to real units.
    If the collection has no images for the month, returns a blank masked image
    instead of crashing.
    """
    filtered = image_collection.select(band_name)

    has_data = filtered.size().gt(0)

    image_with_data    = filtered.mean().multiply(scale_factor).add(offset).rename(output_name)
    image_without_data = ee.Image().rename(output_name).selfMask()

    return ee.Image(ee.Algorithms.If(has_data, image_with_data, image_without_data))


# -------------------------------------------------------
# STEP 4: DEFINE HOW TO PROCESS ONE PROVINCE
# -------------------------------------------------------

def process_province(province_name):
    """
    For a given province, builds a stacked satellite image covering the month,
    then reduces it to per-ward statistics. Returns a FeatureCollection where
    each feature is one ward with all its environmental indicators attached.
    """

    province_wards = wards.filter(ee.Filter.eq('Province', province_name))

    # --- Land Surface Temperature (MODIS MOD11A1) ---
    lst_collection = ee.ImageCollection('MODIS/061/MOD11A1').filterDate(start_date, end_date)
    lst_image = build_safe_band_image(lst_collection, 'LST_Day_1km', 0.02, -273.15, 'LST_Surface_Temp_C')

    # --- Vegetation Index (MODIS MOD13Q1) ---
    ndvi_collection = ee.ImageCollection('MODIS/061/MOD13Q1').filterDate(start_date, end_date)
    ndvi_image = build_safe_band_image(ndvi_collection, 'NDVI', 0.0001, 0, 'NDVI_Vegetation_Index')

    # --- Water Index (MODIS MOD09GA, computed as a normalised difference) ---
    ndwi_collection = ee.ImageCollection('MODIS/061/MOD09GA').filterDate(start_date, end_date)

    ndwi_has_data     = ndwi_collection.size().gt(0)
    ndwi_with_data    = ndwi_collection.map(lambda img: img.normalizedDifference(['sur_refl_b04', 'sur_refl_b02'])).mean().rename('NDWI_Water')
    ndwi_without_data = ee.Image().rename('NDWI_Water').selfMask()
    ndwi_image = ee.Image(ee.Algorithms.If(ndwi_has_data, ndwi_with_data, ndwi_without_data))

    # --- Air Temperature and Soil Moisture (ERA5 reanalysis) ---
    era5_collection = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY").filterDate(start_date, end_date)

    era5_has_data = era5_collection.size().gt(0)
    era5_with_data = (
        era5_collection
        .select(['temperature_2m', 'volumetric_soil_water_layer_1'])
        .mean()
        .subtract(ee.Image([273.15, 0]))          # Convert Kelvin to Celsius for temperature
        .rename(['Air_Temp_C', 'Soil_Moisture'])
    )
    era5_without_data = ee.Image.cat([
        ee.Image().rename('Air_Temp_C').selfMask(),
        ee.Image().rename('Soil_Moisture').selfMask()
    ])
    era5_image = ee.Image(ee.Algorithms.If(era5_has_data, era5_with_data, era5_without_data))

    # --- Stack all bands into one image ---
    full_stack = lst_image.addBands([ndvi_image, ndwi_image, era5_image, population_image, cropland_image])

    # --- Calculate the mean value of each band for every ward polygon ---
    ward_means = (
        full_stack
        .reduceRegions(collection=province_wards, reducer=ee.Reducer.mean(), scale=1000)
        .map(lambda feature: feature.set('Month', month_label))
    )

    # --- Find the most common habitat class in each ward ---
    ward_habitat_modes = habitat_image.reduceRegions(
        collection=province_wards,
        reducer=ee.Reducer.mode(),
        scale=1000
    )

    # --- Join the habitat mode result back onto the main ward statistics ---
    joined_results = ee.Join.saveFirst('habitat_data').apply(
        ward_means,
        ward_habitat_modes,
        ee.Filter.equals(leftField='WardID', rightField='WardID')
    )

    # --- Add a few extra computed fields to each ward feature ---
    def add_extra_fields(feature):
        centroid  = feature.geometry().centroid()
        latitude  = centroid.coordinates().get(1)
        longitude = centroid.coordinates().get(0)

        habitat_code = ee.Feature(feature.get('habitat_data')).get('mode')

        # WorldPop gives population per 100m pixel, so multiply by 100 to get per km².
        population_density = ee.Number(feature.get('Population_Count')).multiply(100)

        return feature.set({
            'Habitat_Class_Code':         habitat_code,
            'Population_Density_Per_KM2': population_density,
            'latitude':                   latitude,
            'longitude':                  longitude
        })

    return joined_results.map(add_extra_fields)


# -------------------------------------------------------
# STEP 5: START AN EXPORT TASK FOR EACH PROVINCE
# -------------------------------------------------------

# These are the columns we want in the exported CSV file.
export_columns = [
    'Month', 'WardID', 'WardLabel', 'Municipali', 'Province',
    'latitude', 'longitude',
    'LST_Surface_Temp_C', 'Air_Temp_C', 'Soil_Moisture',
    'NDWI_Water', 'NDVI_Vegetation_Index',
    'Agric_Percentage', 'Population_Density_Per_KM2',
    'Population_Count', 'Habitat_Class_Code'
]

tasks = []

for province_name in provinces:
    province_slug = province_name.replace(' ', '_')
    filename      = f"{province_slug}_{month_label}"
    ward_data     = process_province(province_name)

    task = ee.batch.Export.table.toCloudStorage(
        collection=ward_data,
        description=f"Export_{filename}",
        bucket=BUCKET_NAME,
        fileNamePrefix=f"pipeline_output/{filename}",
        fileFormat='CSV',
        selectors=export_columns
    )
    task.start()
    tasks.append((task, filename))

    logging.info(f"Export task started for: {province_name}")


# -------------------------------------------------------
# STEP 6: WAIT FOR EACH TASK TO FINISH, THEN LOAD INTO DB
# -------------------------------------------------------

for task, filename in tasks:

    # Poll until the Earth Engine task is done.
    logging.info(f"Waiting for task to finish: {filename}")
    while task.active():
        time.sleep(30)

    if task.status()['state'] != 'COMPLETED':
        logging.error(f"Task failed: {filename}  Status: {task.status()}")
        sys.exit(1)

    logging.info(f"Task finished: {filename}. Loading into database...")

    try:
        # Read the exported CSV from Google Cloud Storage into a DataFrame.
        bucket = storage_client.bucket(BUCKET_NAME)
        blob   = bucket.blob(f"pipeline_output/{filename}.csv")
        df     = pd.read_csv(StringIO(blob.download_as_text()))


        # ---------------------------------------------------
        # BUILD THE 'zone' TABLE ROWS
        # ---------------------------------------------------
        # Each row represents one ward with its geographic and demographic info.

        zone_id               = pd.to_numeric(df['WardID'], errors='coerce')
        zone_name             = df.get('WardLabel', 'Ward ' + df['WardID'].astype(str)).astype(str)
        municipality_name     = df.get('Municipali', '').astype(str)
        province_name_col     = df.get('Province', '').astype(str)
        base_population       = pd.to_numeric(df.get('Population_Count'), errors='coerce').fillna(0).astype(int)
        dominant_habitat_code = df.get('Habitat_Class_Code').astype(str)
        external_zone_id      = df['WardID'].astype(str)
        latitude              = pd.to_numeric(df.get('latitude'),  errors='coerce')
        longitude             = pd.to_numeric(df.get('longitude'), errors='coerce')
        population_density    = pd.to_numeric(df.get('Population_Density_Per_KM2'), errors='coerce')

        df_zone = pd.DataFrame({
            'zone_id':                    zone_id,
            'zone_name':                  zone_name,
            'municipality_name':          municipality_name,
            'province_name':              province_name_col,
            'base_population':            base_population,
            'dominant_habitat_code':      dominant_habitat_code,
            'external_zone_id':           external_zone_id,
            'zone_type':                  'Ward',
            'latitude':                   latitude,
            'longitude':                  longitude,
            'population_density_per_km2': population_density,
        })

        # Remove rows with no ward ID, and drop duplicate wards.
        df_zone = df_zone.dropna(subset=['zone_id'])
        df_zone = df_zone.drop_duplicates(subset=['zone_id'])
        df_zone['zone_id'] = df_zone['zone_id'].astype(int)

        # Replace NaN with None so PostgreSQL receives NULL instead of a string "NaN".
        df_zone = df_zone.where(pd.notnull(df_zone), None)


        # ---------------------------------------------------
        # BUILD THE 'malaria_observation' TABLE ROWS
        # ---------------------------------------------------
        # Each row represents one ward's environmental readings for the month.

        df_obs = pd.DataFrame({
            'date_recorded':         pd.to_datetime(df['Month'] + '-01'),
            'zone_id':               zone_id,
            'lst_surface_temp_c':    df.get('LST_Surface_Temp_C'),
            'air_temp_c':            df.get('Air_Temp_C'),
            'soil_moisture_pct':     df.get('Soil_Moisture'),
            'ndvi_vegetation_index': df.get('NDVI_Vegetation_Index'),
            'ndwi_water_index':      df.get('NDWI_Water'),
            'agric_percentage':      df.get('Agric_Percentage'),
        })

        df_obs = df_obs.dropna(subset=['zone_id', 'date_recorded'])
        df_obs['zone_id'] = df_obs['zone_id'].astype(int)
        df_obs = df_obs.where(pd.notnull(df_obs), None)


        # ---------------------------------------------------
        # INSERT BOTH TABLES INTO POSTGRESQL
        # ---------------------------------------------------

        connection = psycopg2.connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            connect_timeout=10
        )

        try:
            with connection:
                with connection.cursor() as cursor:

                    # Insert or update zone rows.
                    zone_rows = [tuple(row) for row in df_zone[[
                        'zone_id', 'zone_name', 'municipality_name', 'province_name',
                        'base_population', 'dominant_habitat_code', 'external_zone_id',
                        'zone_type', 'latitude', 'longitude', 'population_density_per_km2'
                    ]].to_numpy()]

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
                    """, zone_rows)

                    # Insert or update observation rows.
                    obs_rows = [tuple(row) for row in df_obs[[
                        'date_recorded', 'zone_id', 'lst_surface_temp_c', 'air_temp_c',
                        'soil_moisture_pct', 'ndvi_vegetation_index', 'ndwi_water_index', 'agric_percentage'
                    ]].to_numpy()]

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
                    """, obs_rows, page_size=1000)

            logging.info(f"Successfully loaded into database: {filename}")

        except Exception as db_error:
            connection.rollback()
            logging.error(f"Database error while processing {filename}: {db_error}")
            sys.exit(1)

        finally:
            connection.close()

    except Exception as error:
        logging.error(f"Error while processing {filename}: {error}")
        sys.exit(1)


logging.info("Monthly pipeline complete.")
