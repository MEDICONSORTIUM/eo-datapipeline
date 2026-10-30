"""Earth Engine collection and Cloud Storage export functions for NCD data."""

from datetime import date

import ee

from .sources import KG_M3_TO_UG_M3, UV_DOSE_RATE_TO_INDEX


PROJECT_ID = "modis-temperature-data"
WARD_ASSET = f"projects/{PROJECT_ID}/assets/MDB_Wards_2020"
PROVINCES = [
    "Limpopo",
    "Gauteng",
    "Mpumalanga",
    "North West",
    "Free State",
    "KwaZulu-Natal",
    "Eastern Cape",
    "Western Cape",
    "Northern Cape",
]


def _empty_value(name):
    return ee.Image.constant(0).updateMask(ee.Image.constant(0)).rename(name)


def _mean_and_count(collection, band, output_name, scale=1, offset=0):
    """Return a scaled temporal mean and a valid-sample count band."""
    selected = collection.select(band)
    with_data = ee.Image.cat(
        [
            selected.mean().multiply(scale).add(offset).rename(output_name),
            selected.count().rename(f"{output_name}_samples"),
        ]
    )
    without_data = ee.Image.cat(
        [_empty_value(output_name), ee.Image.constant(0).rename(f"{output_name}_samples")]
    )
    return ee.Image(ee.Algorithms.If(selected.size().gt(0), with_data, without_data))


def _daily_temperature(hourly, start, end):
    start_date = ee.Date(str(start))
    end_date = ee.Date(str(end))
    offsets = ee.List.sequence(0, end_date.difference(start_date, "day").subtract(1))

    def maximum_for_day(offset):
        # South African midnight is 22:00 UTC on the previous date.
        day_start = start_date.advance(offset, "day").advance(-2, "hour")
        day_values = hourly.filterDate(day_start, day_start.advance(1, "day")).select(
            "temperature_2m"
        )
        value = ee.Image(
            ee.Algorithms.If(
                day_values.size().gt(0),
                day_values.max().rename("temperature_2m"),
                _empty_value("temperature_2m"),
            )
        )
        return value.set("system:time_start", day_start.millis())

    return ee.ImageCollection.fromImages(offsets.map(maximum_for_day))


def _quality_mask_no2(image):
    return image.updateMask(image.select("cloud_fraction").lte(0.30))


def _quality_mask_so2(image):
    return image.updateMask(image.select("cloud_fraction").lte(0.30))


def _satellite_mean_and_count(collection, band, output_name, expected_name, quality_mask):
    """Count local source samples before and after the additional cloud filter."""
    filtered = _mean_and_count(collection.map(quality_mask), band, output_name)
    available = _mean_and_count(collection, band, output_name)
    # Zero counts must participate in both ward means, including fully masked pixels.
    # Collection.size() counts unrelated orbits elsewhere in the province.
    return filtered.select(output_name).addBands(
        filtered.select(f"{output_name}_samples").unmask(0, False)
    ).addBands(
        available.select(f"{output_name}_samples").unmask(0, False).rename(expected_name)
    )


def _cams_pm_image(image):
    pm25 = image.select("particulate_matter_d_less_than_25_um_surface").multiply(
        KG_M3_TO_UG_M3
    )
    pm10 = image.select("particulate_matter_d_less_than_10_um_surface").multiply(
        KG_M3_TO_UG_M3
    )
    return ee.Image.cat(
        [
            pm25.rename("pm25_mean_ug_m3"),
            pm10.rename("pm10_mean_ug_m3"),
            pm10.subtract(pm25).rename("pm25_10_mean_ug_m3"),
        ]
    ).copyProperties(image, ["system:time_start"])


def build_ward_collection(start, end, province, ward_limit=None, include_annual=False):
    """Build source-filtered ward summaries at each source's native scale."""
    wards = ee.FeatureCollection(WARD_ASSET).filter(ee.Filter.eq("Province", province))
    if ward_limit:
        wards = wards.limit(ward_limit)
    bounds = wards.geometry()

    local_month_start = ee.Date(str(start)).advance(-2, "hour")
    local_month_end = ee.Date(str(end)).advance(-2, "hour")
    era5 = (
        ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
        .filterDate(local_month_start, local_month_end)
        .filterBounds(bounds)
    )
    era_mean = _mean_and_count(
        era5, "temperature_2m", "air_temp_mean_c", scale=1, offset=-273.15
    )
    daily_temperature = _daily_temperature(era5, start, end)
    era_daily_max = _mean_and_count(
        daily_temperature,
        "temperature_2m",
        "air_temp_daily_max_mean_c",
        scale=1,
        offset=-273.15,
    )
    era_image = era_mean.addBands(era_daily_max)

    no2 = (
        ee.ImageCollection("COPERNICUS/S5P/OFFL/L3_NO2")
        .filterDate(str(start), str(end))
        .filterBounds(bounds)
    )
    no2_image = _satellite_mean_and_count(
        no2,
        "tropospheric_NO2_column_number_density",
        "no2_tropospheric_column_mol_m2",
        "no2_expected_samples",
        _quality_mask_no2,
    )

    so2 = (
        ee.ImageCollection("COPERNICUS/S5P/OFFL/L3_SO2")
        .filterDate(str(start), str(end))
        .filterBounds(bounds)
    )
    so2_image = _satellite_mean_and_count(
        so2, "SO2_column_number_density", "so2_column_mol_m2",
        "so2_expected_samples", _quality_mask_so2,
    )

    cams = (
        ee.ImageCollection("ECMWF/CAMS/NRT")
        .filterDate(str(start), str(end))
        .filter(ee.Filter.eq("model_initialization_hour", 0))
    )
    cams_pm = cams.filter(ee.Filter.lte("model_forecast_hour", 21)).map(_cams_pm_image)
    cams_pm_image = ee.Image.cat(
        [
            _mean_and_count(cams_pm, "pm25_mean_ug_m3", "pm25_mean_ug_m3"),
            _mean_and_count(cams_pm, "pm10_mean_ug_m3", "pm10_mean_ug_m3"),
            _mean_and_count(cams_pm, "pm25_10_mean_ug_m3", "pm25_10_mean_ug_m3"),
        ]
    )
    cams_uv = cams.filter(ee.Filter.eq("model_forecast_hour", 12))
    cams_uv_image = _mean_and_count(
        cams_uv,
        "uv_biologically_effective_dose_surface",
        "uv_index_14h_mean",
        scale=UV_DOSE_RATE_TO_INDEX,
    )

    reduced = era_image.reduceRegions(
        collection=wards, reducer=ee.Reducer.mean(), scale=11132, tileScale=4
    )
    reduced = no2_image.reduceRegions(
        collection=reduced, reducer=ee.Reducer.mean(), scale=3500, tileScale=4
    )
    reduced = so2_image.reduceRegions(
        collection=reduced, reducer=ee.Reducer.mean(), scale=3500, tileScale=4
    )
    reduced = cams_pm_image.addBands(cams_uv_image).reduceRegions(
        collection=reduced, reducer=ee.Reducer.mean(), scale=44528, tileScale=4
    )

    dynamic_world = None
    if include_annual:
        year_start = date(start.year, 1, 1)
        year_end = date(start.year + 1, 1, 1)
        dynamic_world = (
            ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
            .filterDate(str(year_start), str(year_end))
            .filterBounds(bounds)
        )
        built_values = dynamic_world.select("built")
        built_fraction = built_values.mean().gte(0.60).rename("built_up_fraction")
        built_samples = built_values.count().rename("built_up_fraction_samples")
        built_image = ee.Image.cat([built_fraction, built_samples])
        reduced = built_image.reduceRegions(
            collection=reduced, reducer=ee.Reducer.mean(), scale=10, tileScale=8
        )

    expected_days = (end - start).days

    def add_metadata(feature):
        centroid = feature.geometry().centroid(1)
        values = {
            "period_start": str(start),
            "period_end": str(end),
            "ward_area_m2": feature.geometry().area(1),
            "latitude": centroid.coordinates().get(1),
            "longitude": centroid.coordinates().get(0),
            "era5_expected_samples": era5.size(),
            "era5_expected_days": expected_days,
            "cams_pm_expected_samples": cams_pm.size(),
            "cams_uv_expected_samples": cams_uv.size(),
        }
        if include_annual:
            values["dynamic_world_expected_samples"] = dynamic_world.size()
        return feature.set(values)

    return reduced.map(add_metadata)


def export_to_storage(collection, bucket_name, prefix, include_annual=False):
    """Export one province under the protected ncd/ Cloud Storage prefix."""
    if not prefix.startswith("ncd/"):
        raise ValueError("NCD exports must use the ncd/ storage prefix.")
    selectors = [
        "WardID",
        "WardLabel",
        "Municipali",
        "District",
        "Province",
        "latitude",
        "longitude",
        "period_start",
        "period_end",
        "ward_area_m2",
        "air_temp_mean_c",
        "air_temp_mean_c_samples",
        "air_temp_daily_max_mean_c",
        "air_temp_daily_max_mean_c_samples",
        "era5_expected_samples",
        "era5_expected_days",
        "no2_tropospheric_column_mol_m2",
        "no2_tropospheric_column_mol_m2_samples",
        "no2_expected_samples",
        "so2_column_mol_m2",
        "so2_column_mol_m2_samples",
        "so2_expected_samples",
        "pm25_mean_ug_m3",
        "pm25_mean_ug_m3_samples",
        "pm10_mean_ug_m3",
        "pm10_mean_ug_m3_samples",
        "pm25_10_mean_ug_m3",
        "pm25_10_mean_ug_m3_samples",
        "cams_pm_expected_samples",
        "uv_index_14h_mean",
        "uv_index_14h_mean_samples",
        "cams_uv_expected_samples",
    ]
    if include_annual:
        selectors.extend(
            [
                "built_up_fraction",
                "built_up_fraction_samples",
                "dynamic_world_expected_samples",
            ]
        )
    task = ee.batch.Export.table.toCloudStorage(
        collection=collection,
        description="NCD_" + prefix.replace("/", "_")[-80:],
        bucket=bucket_name,
        fileNamePrefix=prefix,
        fileFormat="CSV",
        selectors=selectors,
    )
    task.start()
    return task

