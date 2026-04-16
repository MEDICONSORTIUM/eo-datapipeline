import ee
import os
import sys
import json
import time
import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from io import StringIO
from google.cloud import storage
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
service_account_info = json.loads(os.environ['GCP_SA_JSON'])

SCOPES = [
    'https://www.googleapis.com/auth/earthengine',
    'https://www.googleapis.com/auth/cloud-platform'
]

credentials = service_account.Credentials.from_service_account_info(
    service_account_info, 
    scopes=SCOPES
)

ee.Initialize(credentials=credentials, project='modis-temperature-data')
storage_client = storage.Client(credentials=credentials)
BUCKET_NAME = os.environ['BUCKET_NAME']

# ---------------------------------------
# 1. DATE LOGIC
# ---------------------------------------
now = ee.Date(time.time() * 1000)
end_date = ee.Date(now.format("YYYY-MM-01"))
start_date = end_date.advance(-1, 'month')
month_label = start_date.format('YYYY-MM').getInfo() # YYYY-MM is easier for Pandas to parse

logging.info(f"Running pipeline for {month_label}")

# ---------------------------------------
# 2. GEE PROCESSING LOGIC
# ---------------------------------------
wards = ee.FeatureCollection("projects/modis-temperature-data/assets/MDB_Wards_2020")
provinces = [
    "Limpopo", "Gauteng", "Mpumalanga", "North West", "Free State",
    "KwaZulu-Natal", "Eastern Cape", "Western Cape", "Northern Cape"
]

pop_count_img = (ee.ImageCollection("WorldPop/GP/100m/pop")
                 .filterDate('2020-01-01', '2021-01-01')
                 .mean().rename('Population_Count'))

landcover = ee.ImageCollection("ESA/WorldCover/v100").first()
cropland = landcover.eq(40).rename('Agric_Percentage')
habitat_class = landcover.select('Map').rename('Habitat_Code')

def get_safe_data(collection, band, scale, offset_val, name):
    coll = collection.select(band)
    return ee.Image(ee.Algorithms.If(
        coll.size().gt(0),
        coll.mean().multiply(scale).add(offset_val).rename(name),
        ee.Image().rename(name).selfMask()
    ))

def process_province(province_name):
    province_fc = wards.filter(ee.Filter.eq('Province', province_name))

    lst = get_safe_data(
        ee.ImageCollection('MODIS/061/MOD11A1').filterDate(start_date, end_date),
        'LST_Day_1km', 0.02, -273.15, 'LST_Surface_Temp_C'
    )
    
    ndvi = get_safe_data(
        ee.ImageCollection('MODIS/061/MOD13Q1').filterDate(start_date, end_date),
        'NDVI', 0.0001, 0, 'NDVI_Vegetation_Index'
    )

    ndwi_coll = ee.ImageCollection('MODIS/061/MOD09GA').filterDate(start_date, end_date)
    ndwi = ee.Image(ee.Algorithms.If(
        ndwi_coll.size().gt(0),
        ndwi_coll.map(lambda img: img.normalizedDifference(['sur_refl_b04', 'sur_refl_b02'])).mean().rename('NDWI_Water'),
        ee.Image().rename('NDWI_Water').selfMask()
    ))

    era5_coll = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY").filterDate(start_date, end_date)
    era5 = ee.Image(ee.Algorithms.If(
        era5_coll.size().gt(0),
        era5_coll.select(['temperature_2m', 'volumetric_soil_water_layer_1'])
                 .mean()
                 .subtract(ee.Image([273.15, 0]))
                 .rename(['Air_Temp_C', 'Soil_Moisture']),
        ee.Image.cat([
            ee.Image().rename('Air_Temp_C').selfMask(),
            ee.Image().rename('Soil_Moisture').selfMask()
        ])
    ))

    stack = lst.addBands([ndvi, ndwi, era5, pop_count_img, cropland])
    
    stats_mean = stack.reduceRegions(
        collection=province_fc,
        reducer=ee.Reducer.mean(),
        scale=1000
    ).map(lambda f: f.set('Month', month_label))

    stats_mode = habitat_class.reduceRegions(
        collection=province_fc,
        reducer=ee.Reducer.mode(),
        scale=1000
    )

    join = ee.Join.saveFirst('habitat_data').apply(
        stats_mean, stats_mode, ee.Filter.equals(leftField='WardID', rightField='WardID')
    )

    def finalize(feature):
        centroid = feature.geometry().centroid()
        return feature.set({
            'Habitat_Class_Code': ee.Feature(feature.get('habitat_data')).get('mode'),
            'Population_Density_Per_KM2': ee.Number(feature.get('Population_Count')).multiply(100),
            'latitude': centroid.coordinates().get(1),
            'longitude': centroid.coordinates().get(0)
        })

    return join.map(finalize)

# ---------------------------------------
# 3. EXPORT TASKS
# ---------------------------------------
tasks = []
for prov in provinces:
    prov_clean = prov.replace(' ', '_')
    filename = f"{prov_clean}_{month_label}"
    dataset = process_province(prov)

    task = ee.batch.Export.table.toCloudStorage(
        collection=dataset,
        description=f"Export_{filename}",
        bucket=BUCKET_NAME,
        fileNamePrefix=f"pipeline_output/{filename}",
        fileFormat='CSV',
        selectors=[
            'Month', 'WardID', 'WardLabel', 'latitude', 'longitude', 
            'LST_Surface_Temp_C', 'Air_Temp_C', 'Soil_Moisture', 'NDWI_Water', 
            'NDVI_Vegetation_Index', 'Agric_Percentage', 'Population_Density_Per_KM2', 
            'Population_Count', 'Habitat_Class_Code'
        ]
    )
    task.start()
    tasks.append((task, filename))
    logging.info(f"Started GEE Task: {prov}")

# ---------------------------------------
# 4. WAIT & DB UPLOAD
# ---------------------------------------
for task, filename in tasks:
    logging.info(f"Waiting for task: {filename}...")
    
    while task.active():
        time.sleep(30)

    status = task.status()
    if status['state'] == 'COMPLETED':
        logging.info(f"Task {filename} finished. Starting DB ingestion...")
        
        try:
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"pipeline_output/{filename}.csv")
            df = pd.read_csv(StringIO(blob.download_as_text()))

            # ---------------------------------------
            # PREPARE DATA FOR 'malaria_observation'
            # ---------------------------------------
            df_db = pd.DataFrame()
            df_db['date_recorded'] = pd.to_datetime(df['Month'] + '-01')
            df_db['zone_id'] = pd.to_numeric(df['WardID'], errors='coerce')
            
            df_db = df_db.dropna(subset=['zone_id', 'date_recorded'])
            df_db['zone_id'] = df_db['zone_id'].astype(int)

            df_db['lst_surface_temp_c'] = df.get('LST_Surface_Temp_C')
            df_db['air_temp_c'] = df.get('Air_Temp_C')
            df_db['soil_moisture_pct'] = df.get('Soil_Moisture')
            df_db['ndvi_vegetation_index'] = df.get('NDVI_Vegetation_Index')
            df_db['ndwi_water_index'] = df.get('NDWI_Water')
            df_db['agric_percentage'] = df.get('Agric_Percentage')
            
            # Convert Pandas NaNs to None for Postgres NULL compatibility
            df_db = df_db.where(pd.notnull(df_db), None)

            # ---------------------------------------
            # PREPARE DATA FOR 'zone'
            # ---------------------------------------
            df_zone = pd.DataFrame()
            df_zone['zone_id'] = df_db['zone_id']
            df_zone['zone_name'] = df.get('WardLabel', 'Ward ' + df['WardID'].astype(str)) 
            df_zone['municipality_name'] = df['Municipali'].astype(str)
            df_zone['province_name'] = df['Province'].astype(str)
            df_zone['base_population'] = pd.to_numeric(df.get('Population_Count'), errors='coerce').fillna(0).astype(int)
            df_zone['dominant_habitat_code'] = df.get('Habitat_Class_Code').astype(str)
            df_zone['external_zone_id'] = df['WardID'].astype(str)
            df_zone['zone_type'] = 'Ward'
            df_zone['latitude'] = pd.to_numeric(df.get('latitude'), errors='coerce')
            df_zone['longitude'] = pd.to_numeric(df.get('longitude'), errors='coerce')
            df_zone['population_density_per_km2'] = pd.to_numeric(df.get('Population_Density_Per_KM2'), errors='coerce')
            
            df_zone = df_zone.drop_duplicates(subset=['zone_id'])
            df_zone = df_zone.where(pd.notnull(df_zone), None)

            # ---------------------------------------
            # DATABASE INGESTION
            # ---------------------------------------
            conn = psycopg2.connect(
                host=os.environ['DB_HOST'],
                database=os.environ['DB_NAME'],
                user=os.environ['DB_USER'],
                password=os.environ['DB_PASSWORD'],
                connect_timeout=10
            )
            
            try:
                with conn:
                    with conn.cursor() as cursor:
                        
                        # --- STEP A: UPSERT 'zone' table ---
                       # --- UPSERT 'zone' table ---
                        zone_cols = [
                            'zone_id', 'zone_name', 'municipality_name', 'province_name', 
                            'base_population', 'dominant_habitat_code', 'external_zone_id', 
                            'zone_type', 'latitude', 'longitude', 'population_density_per_km2'
                        ]
                        zone_tuples = [tuple(x) for x in df_zone[zone_cols].to_numpy()]
                        
                        execute_batch(cursor, """
                            INSERT INTO zone (
                                zone_id, zone_name, municipality_name, province_name,
                                base_population, dominant_habitat_code, external_zone_id, 
                                zone_type, latitude, longitude, population_density_per_km2
                            ) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                            ON CONFLICT (zone_id) DO UPDATE SET 
                                zone_name = EXCLUDED.zone_name,
                                municipality_name = EXCLUDED.municipality_name,
                                province_name = EXCLUDED.province_name,
                                base_population = EXCLUDED.base_population,
                                dominant_habitat_code = EXCLUDED.dominant_habitat_code,
                                external_zone_id = EXCLUDED.external_zone_id,
                                latitude = EXCLUDED.latitude,
                                longitude = EXCLUDED.longitude,
                                population_density_per_km2 = EXCLUDED.population_density_per_km2
                        """, zone_tuples)

                        # --- STEP B: UPSERT 'malaria_observation' table ---
                        logging.info("Inserting weather observations...")
                        obs_cols = [
                            'date_recorded', 'zone_id', 'lst_surface_temp_c', 'air_temp_c', 
                            'soil_moisture_pct', 'ndvi_vegetation_index', 'ndwi_water_index', 'agric_percentage'
                        ]
                        obs_tuples = [tuple(x) for x in df_db[obs_cols].to_numpy()]
                        
                        query = """
                            INSERT INTO malaria_observation 
                                (date_recorded, zone_id, lst_surface_temp_c, air_temp_c, 
                                 soil_moisture_pct, ndvi_vegetation_index, ndwi_water_index, agric_percentage)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (zone_id, date_recorded) 
                            DO UPDATE SET 
                                lst_surface_temp_c = EXCLUDED.lst_surface_temp_c,
                                air_temp_c = EXCLUDED.air_temp_c,
                                soil_moisture_pct = EXCLUDED.soil_moisture_pct,
                                ndvi_vegetation_index = EXCLUDED.ndvi_vegetation_index,
                                ndwi_water_index = EXCLUDED.ndwi_water_index,
                                agric_percentage = EXCLUDED.agric_percentage
                        """
                        execute_batch(cursor, query, obs_tuples, page_size=1000)
                        
                logging.info(f"Successfully uploaded {filename}")
            
            except Exception as db_e:
                if conn and not conn.closed: conn.rollback()
                logging.error(f"Database error on {filename}: {db_e}")
                sys.exit(1)
            finally:
                if conn: conn.close()

        except Exception as e:
            logging.error(f"Processing error on {filename}: {e}")
            sys.exit(1)

logging.info("Monthly Pipeline Complete.")
