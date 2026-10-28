-- LEGACY SCHEMA: does not describe the current eoha schema in RDS.
-- database.py deliberately does not execute this file.
CREATE TABLE IF NOT EXISTS public.zone (
    zone_id INTEGER PRIMARY KEY,
    zone_name TEXT,
    municipality_name TEXT,
    province_name TEXT,
    base_population INTEGER,
    dominant_habitat_code TEXT,
    external_zone_id TEXT,
    zone_type TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    population_density_per_km2 DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS public.malaria_observation (
    date_recorded DATE NOT NULL,
    zone_id INTEGER NOT NULL REFERENCES public.zone (zone_id),
    lst_surface_temp_c DOUBLE PRECISION,
    air_temp_c DOUBLE PRECISION,
    -- Legacy name: values are volumetric soil water (m3/m3), not percentages.
    soil_moisture_pct DOUBLE PRECISION,
    ndvi_vegetation_index DOUBLE PRECISION,
    ndwi_water_index DOUBLE PRECISION,
    -- Stored as a fraction, from 0 to 1.
    agric_percentage DOUBLE PRECISION,
    PRIMARY KEY (zone_id, date_recorded)
);
