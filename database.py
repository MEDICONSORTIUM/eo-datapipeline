"""Connect to the EOHA database and check its existing tables."""

import argparse
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv


def connect_database():
    """Connect using DB_* environment variables, with SSL required by default."""
    load_dotenv(Path(__file__).with_name('.env'), override=False)
    required = ('DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD')
    missing = [name for name in required if not os.environ.get(name, '').strip()]
    if missing:
        raise ValueError('Missing environment variables: ' + ', '.join(missing))
    host = os.environ['DB_HOST'].strip()
    if '://' in host or '/' in host or ':' in host or any(c.isspace() for c in host):
        raise ValueError('DB_HOST must contain only the RDS hostname, without a URL or port.')
    return psycopg2.connect(
        host=host,
        port=os.environ.get('DB_PORT', '5432'),
        dbname=os.environ['DB_NAME'].strip(),
        user=os.environ['DB_USER'].strip(),
        password=os.environ['DB_PASSWORD'],
        sslmode=os.environ.get('DB_SSLMODE', 'require'),
        connect_timeout=10,
        options='-c search_path=eoha,public',
    )


def check_database(connection):
    """Check the EOHA tables without inserting, updating, or creating anything."""
    tables = {
        'data_sources': 'data_source_id, source_code, source_name',
        'environmental_variables': 'variable_id, variable_code, variable_name, unit',
        'regions': 'region_id, region_code, region_name, region_type, province, geometry',
        'environmental_observations': (
            'observation_id, region_id, variable_id, data_source_id, observation_month, '
            'statistic, value, sample_count, cloud_coverage, quality_flag, etl_run_id, metadata'
        ),
        'etl_runs': (
            'etl_run_id, pipeline_name, run_started_at, run_finished_at, '
            'observation_month, status, records_inserted, records_updated, '
            'records_failed, error_message, metadata'
        ),
    }
    with connection.cursor() as cursor:
        cursor.execute('SELECT current_database(), current_user')
        identity = cursor.fetchone()
        if identity[0] != 'eoha':
            raise ValueError('Expected the eoha database. Set DB_NAME=eoha.')
        for table, columns in tables.items():
            cursor.execute(sql.SQL('SELECT {} FROM eoha.{} LIMIT 0').format(
                sql.SQL(columns), sql.Identifier(table)
            ))
            cursor.execute('''
                SELECT has_table_privilege(current_user, %s, 'INSERT')
                   AND has_table_privilege(current_user, %s, 'UPDATE')
            ''', (f'eoha.{table}', f'eoha.{table}'))
            if not cursor.fetchone()[0]:
                raise ValueError(f'Database user needs INSERT and UPDATE on eoha.{table}.')
    return identity


def initialize_database(connection):
    """Prevent the legacy schema file from creating tables in the new database."""
    raise ValueError(
        'The EOHA schema is already managed in RDS. '
        'Run python database.py without --init; schema.sql describes the old tables.'
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--init', action='store_true', help='Unsupported for the existing EOHA schema.')
    args = parser.parse_args()
    connection = None
    try:
        if args.init:
            initialize_database(None)
        connection = connect_database()
        connection.set_session(readonly=True)
        with connection:
            database_name, username = check_database(connection)
        print(f'Database ready: {database_name} (user: {username}). All five EOHA tables accessible.')
    except (ValueError, psycopg2.Error) as error:
        # libpq does not include the password in connection error messages.
        parser.exit(1, f'Database check failed: {error}\n')
    finally:
        if connection is not None:
            connection.close()


if __name__ == '__main__':
    main()
