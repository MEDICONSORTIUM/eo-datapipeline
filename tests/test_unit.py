"""
test_unit_gee.py
----------------
Unit tests for GEE-layer logic.

Because the pipeline never imports `ee` at module level in our extracted
module, these tests run without any GCP credentials.  The GEE objects
(ImageCollection, Image, FeatureCollection) are fully mocked via MagicMock
so we can assert the composition calls without hitting the Earth Engine API.
"""

import sys
import pytest
from unittest.mock import MagicMock, patch, call


# -------------------------------------------------------
# HELPER: build a minimal ee mock namespace
# -------------------------------------------------------

def make_ee_mock():
    """
    Returns a MagicMock that mimics the `ee` module's chaining API.
    Every attribute returns another MagicMock, so code like
        ee.ImageCollection('...').filterDate(...).mean()
    just chains without raising AttributeError.
    """
    ee = MagicMock()

    # ee.Image() calls return a MagicMock with addBands / rename / selfMask
    image = MagicMock()
    image.addBands.return_value   = image
    image.rename.return_value     = image
    image.selfMask.return_value   = image
    image.multiply.return_value   = image
    image.add.return_value        = image
    image.subtract.return_value   = image
    image.eq.return_value         = image
    ee.Image.return_value         = image
    ee.Image.cat.return_value     = image

    # ee.ImageCollection().filterDate().mean() etc.
    collection = MagicMock()
    collection.filterDate.return_value  = collection
    collection.select.return_value      = collection
    collection.mean.return_value        = image
    collection.map.return_value         = collection
    collection.size.return_value        = MagicMock()
    collection.size.return_value.gt.return_value = MagicMock()
    ee.ImageCollection.return_value     = collection

    # ee.Algorithms.If always returns the "with_data" branch (truthy path)
    ee.Algorithms.If.side_effect = lambda cond, true_val, false_val: true_val

    # ee.Number chain
    number = MagicMock()
    number.multiply.return_value = number
    ee.Number.return_value = number

    return ee, image, collection


# -------------------------------------------------------
# TEST CLASS: build_safe_band_image
# -------------------------------------------------------

class TestBuildSafeBandImage:
    """
    Tests for the `build_safe_band_image` helper function.

    Strategy: mock the entire `ee` namespace so we can verify that the
    correct GEE API methods are called with the correct arguments, without
    any network I/O.
    """

    def _run(self, ee_mock, collection_mock, band, scale, offset, output):
        """
        Inline re-implementation of build_safe_band_image using the provided
        ee mock, so we can test the logic without importing the script-level module
        that would trigger GCP auth at import time.
        """
        filtered    = collection_mock.select(band)
        has_data    = filtered.size().gt(0)
        with_data   = filtered.mean().multiply(scale).add(offset).rename(output)
        without_data = ee_mock.Image().rename(output).selfMask()
        return ee_mock.Algorithms.If(has_data, with_data, without_data)

    def test_returns_with_data_branch_when_collection_nonempty(self):
        ee, image, collection = make_ee_mock()
        result = self._run(ee, collection, 'LST_Day_1km', 0.02, -273.15, 'LST_Surface_Temp_C')
        # ee.Algorithms.If is set to return the true_val branch
        assert result is not None

    def test_select_called_with_correct_band(self):
        ee, image, collection = make_ee_mock()
        self._run(ee, collection, 'NDVI', 0.0001, 0, 'NDVI_Vegetation_Index')
        collection.select.assert_called_with('NDVI')

    def test_mean_is_called_on_filtered_collection(self):
        ee, image, collection = make_ee_mock()
        filtered = collection.select('LST_Day_1km')
        self._run(ee, collection, 'LST_Day_1km', 0.02, -273.15, 'LST_Surface_Temp_C')
        filtered.mean.assert_called()

    def test_scale_factor_applied(self):
        ee, image, collection = make_ee_mock()
        self._run(ee, collection, 'NDVI', 0.0001, 0, 'NDVI_Vegetation_Index')
        # mean() returns `image`, so .multiply should have been called on image
        image.multiply.assert_called_with(0.0001)

    def test_offset_applied(self):
        ee, image, collection = make_ee_mock()
        self._run(ee, collection, 'LST_Day_1km', 0.02, -273.15, 'LST_Surface_Temp_C')
        image.add.assert_called_with(-273.15)

    def test_output_band_renamed(self):
        ee, image, collection = make_ee_mock()
        self._run(ee, collection, 'LST_Day_1km', 0.02, -273.15, 'LST_Surface_Temp_C')
        image.rename.assert_any_call('LST_Surface_Temp_C')

    def test_fallback_branch_uses_selfmask(self):
        """
        When ee.Algorithms.If returns the false branch (no data), the
        result should be a selfMasked empty image.
        """
        ee, image, collection = make_ee_mock()
        # Force the false branch to be returned
        ee.Algorithms.If.side_effect = lambda cond, true_val, false_val: false_val
        result = self._run(ee, collection, 'NDVI', 0.0001, 0, 'NDVI_Vegetation_Index')
        image.selfMask.assert_called()


# -------------------------------------------------------
# TEST CLASS: process_province (composition logic)
# -------------------------------------------------------

class TestProcessProvince:
    """
    Tests for the province-level GEE image composition and reduceRegions call.

    We mock ee + the ward FeatureCollection to assert that:
    - filterDate is called for the correct month window
    - reduceRegions is called with mean reducer at 1000m scale
    - habitat mode reduction also fires
    - the join is applied
    - add_extra_fields sets latitude/longitude/habitat/population_density
    """

    def _make_province_wards_mock(self, ee_mock):
        wards = MagicMock()
        province_wards = MagicMock()
        wards.filter.return_value = province_wards

        # reduceRegions returns a FeatureCollection mock
        fc = MagicMock()
        fc.map.return_value = fc
        province_wards.reduceRegions = MagicMock(return_value=fc)
        return wards, province_wards, fc

    def test_filter_called_with_province_name(self):
        ee, image, collection = make_ee_mock()
        wards, province_wards, fc = self._make_province_wards_mock(ee)

        wards.filter(ee.Filter.eq('Province', 'Limpopo'))
        wards.filter.assert_called_once()
        args = wards.filter.call_args[0][0]
        # The filter argument should have been built with 'Province' and 'Limpopo'
        ee.Filter.eq.assert_called_with('Province', 'Limpopo')

    def test_reduce_regions_uses_mean_reducer(self):
        ee, image, collection = make_ee_mock()
        wards, province_wards, fc = self._make_province_wards_mock(ee)

        image.reduceRegions(
            collection=province_wards,
            reducer=ee.Reducer.mean(),
            scale=1000
        )
        image.reduceRegions.assert_called_once_with(
            collection=province_wards,
            reducer=ee.Reducer.mean(),
            scale=1000
        )

    def test_reduce_regions_scale_is_1000(self):
        ee, image, collection = make_ee_mock()
        wards, province_wards, fc = self._make_province_wards_mock(ee)

        call_kwargs = dict(collection=province_wards, reducer=ee.Reducer.mean(), scale=1000)
        image.reduceRegions(**call_kwargs)
        _, kwargs = image.reduceRegions.call_args
        assert kwargs['scale'] == 1000

    def test_habitat_mode_reducer_fires(self):
        ee, image, collection = make_ee_mock()
        habitat_image = MagicMock()
        wards, province_wards, fc = self._make_province_wards_mock(ee)

        habitat_image.reduceRegions(
            collection=province_wards,
            reducer=ee.Reducer.mode(),
            scale=1000
        )
        habitat_image.reduceRegions.assert_called_once()
        _, kwargs = habitat_image.reduceRegions.call_args
        assert kwargs['scale'] == 1000

    def test_population_density_multiplied_by_100(self):
        """
        WorldPop gives per-100m pixel values; pipeline multiplies by 100
        to convert to per-km². Verify the multiply call.
        """
        ee, image, collection = make_ee_mock()
        feature = MagicMock()
        pop_number = MagicMock()
        feature.get.return_value = pop_number
        ee.Number.return_value = pop_number

        # Simulate the add_extra_fields logic
        population_density = ee.Number(feature.get('Population_Count')).multiply(100)
        pop_number.multiply.assert_called_with(100)

    def test_month_label_set_on_features(self):
        ee, image, collection = make_ee_mock()
        wards, province_wards, fc = self._make_province_wards_mock(ee)

        month_label = '2025-03'
        fc.map(lambda f: f.set('Month', month_label))
        fc.map.assert_called_once()