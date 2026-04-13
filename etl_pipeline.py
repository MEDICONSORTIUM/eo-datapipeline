import ee as status
import os
import json
import time
import logging
import pandas as pd
import psycopg2
from io import StringIO
from google.oauth2 import service_account
from google.cloud import storage

# ---------------------------------------
# 0. LOGGING SETUP
# ---------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------
# 1. AUTHENTICATION & INITIALIZATION
# ---------------------------------------
service_account_info = json.loads(os.environ['GCP_SA_JSON'])

# Define BOTH scopes in one list to avoid the 'invalid_scope' error
scopes = [
    'https://www.googleapis.com/auth/earthengine',
    'https://www.googleapis.com/auth/cloud-platform'
]

# Create ONE credentials object for both EE and GCS
credentials = service_account.Credentials.from_service_account_info(
    service_account_info, 
    scopes=scopes
)

# Initialize clients using the scoped credentials
status.Initialize(credentials=credentials, project='modis-temperature-data')
storage_client = storage.Client(credentials=credentials)

BUCKET_NAME = os.environ['BUCKET_NAME']

# ---------------------------------------
# 2. DATE LOGIC
# ---------------------------------------
now = status.Date(time.time() * 1000)
end_date = status.Date(now.format("YYYY-MM-01"))
start_date = end_date.advance(-1, 'month')
month_label = start_date.format('YYYY-MM').getInfo()

logging.info(f"Running pipeline for {month_label}")

# ---------------------------------------
# 3. PROCESSING LOGIC
# ---------------------------------------
wards = status.FeatureCollection("projects/modis-temperature-data/assets/MDB_Wards_2020")
provinces = ["Limpopo", "Gauteng", "Mpumalanga", "North West", "Free State", "KwaZulu-Natal", "Eastern Cape", "Western Cape", "Northern Cape"]

pop_count_img = status.ImageCollection("WorldPop/GP/100m/pop").filterDate('2020-01-01', '2021-01-01').mean().rename('Population_Count')
landcover = status.ImageCollection("ESA/WorldCover/v100").first()
cropland = landcover.eq(40).rename('Agric_Percentage')

def get_safe_data(collection, band, scale, offset_val, name):
    coll = collection.select(band)
    return status.Image(status.Algorithms.If(
        coll.size().gt(0),
        coll.mean().multiply(scale).add(offset_val).rename(name),
        status.Image().rename(name).selfMask()
    ))

def process_province(province_name):
    province_fc = wards.filter(status.Filter.eq('Province', province_name))
    
    lst = get_safe_data(status.ImageCollection('MODIS/061/MOD11A1').filterDate(start_date, end_date), 'LST_Day_1km', 0.02, -273.15, 'LST_Surface_Temp_C')
    ndvi = get_safe_data(status.ImageCollection('MODIS/061/MOD13Q1').filterDate(start_date, end_date), 'NDVI', 0.0001, 0, 'NDVI_Vegetation_Index')
    
    era5_coll = status.ImageCollection("ECMWF/ERA5_LAND/HOURLY").filterDate(start_date, end_date)
    era5 = status.Image(status.Algorithms.If(
        era5_coll.size().gt(0),
        era5_coll.select(['temperature_2m', 'volumetric_soil_water_layer_1']).mean().subtract(status.Image([273.15, 0])).rename(['Air_Temp_C', 'Soil_Moisture']),
        status.Image.cat([status.Image().rename('Air_Temp_C').selfMask(), status.Image().rename('Soil_Moisture').selfMask()])
    ))

    stack = lst.addBands([ndvi, era5, pop_count_img, cropland])
    stats = stack.reduceRegions(collection=province_fc, reducer=status.Reducer.mean(), scale=1000).map(lambda f: f.set('Month', month_label))
    
    return stats

# ---------------------------------------
# 4. EXPORT TASKS
# ---------------------------------------
tasks = []
for prov in provinces:
    prov_clean = prov.replace(' ', '_')
    filename = f"{prov_clean}_{month_label}"
    dataset = process_province(prov)
    
    task = status.batch.Export.table.toCloudStorage(
        collection=dataset,
        description=f"Export_{filename}",
        bucket=BUCKET_NAME,
        fileNamePrefix=f"pipeline_output/{filename}",
        fileFormat='CSV'
    )
    task.start()
    tasks.append((task, filename))
    logging.info(f"Started GEE Task: {prov}")

# ---------------------------------------
# 5. WAIT & DB UPLOAD
# ---------------------------------------
for task, filename in tasks:
    while task.active():
        time.sleep(30)
    
    if task.status()['state'] == 'COMPLETED':
        logging.info(f"Ingesting {filename}...")
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"pipeline_output/{filename}.csv")
        content = blob.download_as_text()
        
        df = pd.read_csv(StringIO(content))
        
        # Prepare for Database (malaria_observation table)
        df_db = pd.DataFrame()
        df_db['date_recorded'] = df['Month'] + "-01"
        df_db['zone_id'] = pd.to_numeric(df['WardID'], errors='coerce')
        df_db['lst_surface_temp_c'] = df['LST_Surface_Temp_C']
        df_db['air_temp_c'] = df['Air_Temp_C']
        df_db = df_db.dropna(subset=['zone_id'])

        try:
            conn = psycopg2.connect(
                host=os.environ['DB_HOST'],
                database=os.environ['DB_NAME'],
                user=os.environ['DB_USER'],
                password=os.environ['DB_PASSWORD']
            )
            cursor = conn.cursor()
            
            # Use high-performance COPY
            buffer = StringIO()
            df_db.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            cursor.copy_expert("""
                COPY malaria_observation (date_recorded, zone_id, lst_surface_temp_c, air_temp_c) 
                FROM STDIN WITH CSV
            """, buffer)
            
            conn.commit()
            cursor.close()
            conn.close()
            logging.info(f"Successfully uploaded {filename}")
        except Exception as e:
            logging.error(f"Database error on {filename}: {e}")

logging.info("Monthly Pipeline Complete.")
