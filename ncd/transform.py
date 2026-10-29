"""Pure validation and transformation functions for NCD exports."""

import json
from datetime import date

import pandas as pd

from .sources import INDICATORS, SOURCE_VERSIONS


REQUIRED_EXPORT_COLUMNS = {
    "WardID",
    "period_start",
    "period_end",
    "ward_area_m2",
}


def validate_export(frame, requested_start=None, requested_end=None):
    """Validate the minimum Earth Engine export contract."""
    missing = REQUIRED_EXPORT_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError("NCD export is missing columns: " + ", ".join(sorted(missing)))
    ward_codes = frame["WardID"].dropna().astype(str).str.removesuffix(".0")
    if ward_codes.duplicated().any():
        raise ValueError("NCD export contains duplicate WardID rows.")
    if ward_codes.empty:
        raise ValueError("NCD export contains no ward rows.")
    if requested_start is not None and requested_end is not None:
        starts = set(pd.to_datetime(frame["period_start"], errors="coerce").dt.date.dropna())
        ends = set(pd.to_datetime(frame["period_end"], errors="coerce").dt.date.dropna())
        if starts != {requested_start} or ends != {requested_end}:
            raise ValueError("NCD export period does not match the requested month.")


def _number(item, column):
    value = pd.to_numeric(item.get(column), errors="coerce")
    return None if pd.isna(value) else float(value)


def _period(requested_start, requested_end, frequency):
    if frequency == "annual":
        return date(requested_start.year, 1, 1), date(requested_start.year + 1, 1, 1)
    return requested_start, requested_end


def _quality(
    value,
    sample_count,
    expected_count,
    minimum_coverage,
    minimum_value=None,
    maximum_value=None,
):
    if expected_count is None or expected_count <= 0:
        return None, "NO_SOURCE_DATA", None
    coverage = None if sample_count is None else min(max(sample_count / expected_count, 0.0), 1.0)
    if value is None:
        return None, "MISSING", coverage
    if minimum_value is not None and value < minimum_value:
        return None, "OUT_OF_RANGE", coverage
    if maximum_value is not None and value > maximum_value:
        return None, "OUT_OF_RANGE", coverage
    if coverage is None:
        return None, "UNKNOWN_COVERAGE", None
    if coverage < minimum_coverage:
        return None, "LOW_COVERAGE", coverage
    return value, "PASS", coverage


def build_observation_frame(frame, requested_start, requested_end, etl_run_id, include_annual=False):
    """Convert one wide ward export into normalized NCD observations."""
    validate_export(frame, requested_start, requested_end)
    rows = []
    for _, item in frame.iterrows():
        ward_id = item.get("WardID")
        if pd.isna(ward_id):
            continue
        region_code = str(ward_id).removesuffix(".0")
        ward_area = _number(item, "ward_area_m2")
        for indicator in INDICATORS:
            if indicator["frequency"] == "annual" and not include_annual:
                continue
            value = _number(item, indicator["column"])
            if indicator["variable_code"] == "built_up_area_m2":
                fraction = _number(item, "built_up_fraction")
                value = None if fraction is None or ward_area is None else fraction * ward_area
            samples = _number(item, indicator["sample_column"])
            expected = _number(item, indicator["expected_column"])
            value, quality_flag, coverage = _quality(
                value,
                samples,
                expected,
                indicator["minimum_coverage"],
                indicator.get("minimum_value"),
                indicator.get("maximum_value"),
            )
            period_start, period_end = _period(
                requested_start, requested_end, indicator["frequency"]
            )
            metadata = {
                "frequency": indicator["frequency"],
                "minimum_coverage": indicator["minimum_coverage"],
                "source_version": SOURCE_VERSIONS[indicator["source_code"]],
                "period_end_is_exclusive": True,
            }
            rows.append(
                {
                    "region_code": region_code,
                    "variable_code": indicator["variable_code"],
                    "source_code": indicator["source_code"],
                    "period_start": period_start,
                    "period_end": period_end,
                    "statistic": indicator["statistic"],
                    "value": value,
                    "sample_count": samples,
                    "coverage_fraction": coverage,
                    "quality_flag": quality_flag,
                    "method_version": "ncd-v1",
                    "boundary_version": "MDB_Wards_2020",
                    "etl_run_id": etl_run_id,
                    "processing_metadata": json.dumps(metadata, sort_keys=True),
                }
            )
    result = pd.DataFrame(rows)
    return result.astype(object).where(pd.notnull(result), None)

