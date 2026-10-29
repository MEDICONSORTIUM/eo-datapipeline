"""Separate NCD collection, staging, validation, and database load flow."""

import argparse
import json
import logging
import os
from datetime import date
from io import StringIO
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from database import connect_database
from etl_pipeline import create_google_clients, wait_for_export

from . import database as ncd_database
from .earth_engine import PROJECT_ID, PROVINCES, build_ward_collection, export_to_storage
from .transform import build_observation_frame


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def month_range(month_text=None):
    """Return a requested month as a half-open date range."""
    if month_text:
        try:
            start = date.fromisoformat(f"{month_text}-01")
        except ValueError as error:
            raise ValueError("Month must use YYYY-MM format.") from error
    else:
        today = date.today()
        current = date(today.year, today.month, 1)
        start = date(current.year - 1, 12, 1) if current.month == 1 else date(
            current.year, current.month - 1, 1
        )
    end = date(start.year + 1, 1, 1) if start.month == 12 else date(
        start.year, start.month + 1, 1
    )
    return start, end


def parse_args(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", help="Month to process, for example 2026-01.")
    parser.add_argument("--province", choices=PROVINCES, help="Process only one province.")
    parser.add_argument("--smoke-test", action="store_true", help="Process three wards only.")
    parser.add_argument("--bucket", help="Cloud Storage bucket; overrides BUCKET_NAME.")
    return parser.parse_args(arguments)


def _write_manifest(bucket, prefix, content):
    manifest = bucket.blob(f"{prefix}.manifest.json")
    manifest.upload_from_string(json.dumps(content, indent=2, sort_keys=True), "application/json")


def main(arguments=None):
    """Run the approved NCD pipeline without changing malaria tables."""
    args = parse_args(arguments)
    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    bucket_name = args.bucket or os.environ.get("BUCKET_NAME")
    if not bucket_name:
        raise ValueError("Set BUCKET_NAME or pass --bucket.")
    start, end = month_range(args.month)
    if start < date(2026, 1, 1):
        raise ValueError("The approved NCD backfill starts in January 2026.")
    today = date.today()
    current_month = date(today.year, today.month, 1)
    if start >= current_month:
        raise ValueError("Choose a completed calendar month for NCD processing.")
    include_annual = start.month == 12
    provinces = [args.province] if args.province else PROVINCES
    if args.smoke_test and not args.province:
        provinces = [PROVINCES[0]]

    connection = connect_database()
    etl_run_id = None
    inserted = 0
    updated = 0
    failed = 0
    try:
        ncd_database.check_database(connection)
        etl_run_id = ncd_database.start_run(connection, start, args.province, args.smoke_test)

        storage_client = create_google_clients(os.environ.get("GCP_PROJECT", PROJECT_ID))
        bucket = storage_client.bucket(bucket_name)
        if not bucket.exists():
            raise ValueError(f"Cloud Storage bucket not found: {bucket_name}")

        tasks = []
        for province in provinces:
            province_slug = province.replace(" ", "_")
            suffix = "_smoke" if args.smoke_test else ""
            prefix = f"ncd/{start:%Y-%m}/{province_slug}/run_{etl_run_id}{suffix}"
            collection = build_ward_collection(
                start,
                end,
                province,
                ward_limit=3 if args.smoke_test else None,
                include_annual=include_annual,
            )
            task = export_to_storage(collection, bucket_name, prefix, include_annual)
            tasks.append((task, province, prefix))
            logging.info("NCD export started: %s", prefix)

        for task, province, prefix in tasks:
            wait_for_export(task)
            frame = pd.read_csv(StringIO(bucket.blob(f"{prefix}.csv").download_as_text()))
            observations = build_observation_frame(
                frame, start, end, etl_run_id, include_annual=include_annual
            )
            new_rows, changed_rows = ncd_database.load_observations(connection, observations)
            inserted += new_rows
            updated += changed_rows
            _write_manifest(
                bucket,
                prefix,
                {
                    "boundary_version": "MDB_Wards_2020",
                    "include_annual": include_annual,
                    "method_version": "ncd-v1",
                    "observation_rows": len(observations),
                    "period_end_exclusive": str(end),
                    "period_start": str(start),
                    "province": province,
                    "run_id": etl_run_id,
                },
            )
            logging.info("NCD load complete for %s: %s observations", province, len(observations))

        ncd_database.finish_run(
            connection, etl_run_id, "SUCCESS", inserted, updated, failed
        )
        logging.info("NCD pipeline complete. Inserted: %s, updated: %s", inserted, updated)
    except Exception as error:
        failed += 1
        if etl_run_id is not None:
            final_status = "PARTIAL" if inserted or updated else "FAILED"
            ncd_database.finish_run(
                connection, etl_run_id, final_status, inserted, updated, failed, str(error)
            )
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        logging.error("NCD pipeline failed: %s", error)
        raise SystemExit(1)

