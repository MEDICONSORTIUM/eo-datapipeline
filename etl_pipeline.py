import ee as status  # Keeping your 'status' alias
import os
import json
import time
import pandas as pd
import psycopg2
from google.oauth2 import service_account
from google.cloud import storage

# ---------------------------------------
# 1. AUTHENTICATION & INITIALIZATION
# ---------------------------------------
# Load credentials from GitHub Secret
service_account_info = json.loads(os.environ['GCP_SA_JSON'])
credentials = service_account.Credentials.from_service_account_info(service_account_info)
status.Initialize(credentials=credentials, project='modis-temperature-data')

# Cloud Storage Client
storage_client = storage.Client(credentials=credentials)
BUCKET_NAME = 'YOUR_BUCKET_NAME_HERE' 

# ---------------------------------------
# 2. DYNAMIC DATE LOGIC (Fetch Previous Month)
# ---------------------------------------
# This ensures we always get the most recent completed month
now = status.Date(time.time() * 1000)
end_date = status.Date(now.format("YYYY-MM-01")) # First of this month
start_date = end_date.advance(-1, 'month')      # First of last month
month_label = start_date.format('MMM_YYYY').getInfo()

print(f"Running pipeline for: {month_label}")

# ---------------------------------------
# 3. LOAD DATA (Your GEE Logic)
# ---------------------------------------
wards = status.FeatureCollection("projects/modis-temperature-data/assets/MDB_Wards_2020")
provinces = ["Limpopo", "Gauteng", "Mpumalanga", "North West", "Free State", "KwaZulu-Natal", "Eastern Cape", "Western Cape", "Northern Cape"]

pop_count_img = status.ImageCollection("WorldPop/GP/100m/pop").filterDate('2020-01-01', '2021-01-01').mean().rename('Population_Count')
landcover = status.ImageCollection("ESA/WorldCover/v100").first()
cropland = landcover.eq(40).rename('Agric_Percentage')
habitat_class = landcover.select('Map').rename('Habitat_Code')

def process_province(province_name):
    province_fc = wards.filter(status.Filter.eq('Province', province_name))
    
    # Helper for MODIS/ERA5
    def get_safe_data(collection, band, scale, offset_val, name):
        coll = collection.select(band)
        return status.Image(status.Algorithms.If(
            coll.size().gt(0),
            coll.mean().multiply(scale).add(offset_val).rename(name),
            status.Image().rename(name).selfMask()
        ))

    lst = get_safe_data(status.ImageCollection('MODIS/061/MOD11A1').filterDate(start_date, end_date), 'LST_Day_1km', 0.02, -273.15, 'LST_Surface_C')
    ndvi = get_safe_data(status.ImageCollection('MODIS/061/MOD13Q1').filterDate(start_date, end_date), 'NDVI', 0.0001, 0, 'Habitat_Vegetation_Index')
    
    era5_coll = status.ImageCollection("ECMWF/ERA5_LAND/HOURLY").filterDate(start_date, end_date)
    era5 = status.Image(status.Algorithms.If(
        era5_coll.size().gt(0),
        era5_coll.select(['temperature_2m', 'volumetric_soil_water_layer_1']).mean().subtract(status.Image([273.15, 0])).rename(['Air_Temp_C', 'Soil_Moisture']),
        status.Image.cat([status.Image().rename('Air_Temp_C').selfMask(), status.Image().rename('Soil_Moisture').selfMask()])
    ))

    stack = lst.addBands([ndvi, era5, pop_count_img, cropland])
    
    stats = stack.reduceRegions(collection=province_fc, reducer=status.Reducer.mean(), scale=1000).map(lambda f: f.set('Month', month_label))
    
    # Simplified cleanup for RDS
    def finalize(f):
        return f.set({
            'latitude': f.geometry().centroid().coordinates().get(1),
            'longitude': f.geometry().centroid().coordinates().get(0),
            'Population_Density_Per_KM2': status.Number(f.get('Population_Count')).multiply(100)
        })

    return stats.map(finalize)

# ---------------------------------------
# 4. EXPORT & DATABASE UPLOAD
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
    print(f"Started GEE task for {prov}")

# Wait for tasks and then push to RDS
print("Waiting for Earth Engine to finish processing...")
for task, filename in tasks:
    while task.active():
        time.sleep(30)
    
    if task.status()['state'] == 'COMPLETED':
        print(f"Uploading {filename} to AWS RDS...")
        # Download from GCS
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"pipeline_output/{filename}.csv")
        content = blob.download_as_text()
        
        # Simple RDS Push using Psycopg2
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD']
        )
        df = pd.read_csv(pd.compat.StringIO(content))
        
        # Clean data (Transform)
        df = df.dropna(subset=['WardID']) # Example clean
        
        # Push to RDS (Assuming table 'malaria_data' exists)
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO malaria_data (month, ward_id, lst_c, air_temp) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (row['Month'], row['WardID'], row['LST_Surface_C'], row['Air_Temp_C'])
            )
        conn.commit()
        cursor.close()
        conn.close()