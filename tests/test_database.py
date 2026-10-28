"""Offline checks for replacement database setup and fail-fast behavior."""

import os
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

import database
import etl_pipeline as pipeline


@patch('database.load_dotenv')
class TestConnectDatabase(unittest.TestCase):
    """Connection validation and SSL defaults."""

    def test_missing_settings_do_not_attempt_connection(self, load_dotenv):
        """
        test_missing_settings_do_not_attempt_connection()
        -------------------------------------------------
        Missing configuration must fail before a network request.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts validation and no connection attempt.
        """
        with patch.dict(os.environ, {}, clear=True), patch('database.psycopg2.connect') as connect:
            with self.assertRaisesRegex(ValueError, 'DB_HOST'):
                database.connect_database()
            connect.assert_not_called()

    def test_ssl_and_timeout_with_unmodified_password(self, load_dotenv):
        """
        test_ssl_and_timeout_with_unmodified_password()
        ----------------------------------------------
        Connections require SSL and preserve all password characters.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts connection parameters.
        """
        settings = dict(DB_HOST=' example.rds.amazonaws.com ', DB_NAME='eoha',
                        DB_USER='postgres', DB_PASSWORD=' spaces matter ')
        with patch.dict(os.environ, settings, clear=True), patch('database.psycopg2.connect') as connect:
            database.connect_database()
            self.assertEqual(connect.call_args.kwargs['sslmode'], 'require')
            self.assertEqual(connect.call_args.kwargs['connect_timeout'], 10)
            self.assertEqual(connect.call_args.kwargs['host'], 'example.rds.amazonaws.com')
            self.assertEqual(connect.call_args.kwargs['password'], ' spaces matter ')
            self.assertEqual(connect.call_args.kwargs['dbname'], 'eoha')
            self.assertEqual(connect.call_args.kwargs['options'], '-c search_path=eoha,public')
            self.assertFalse(load_dotenv.call_args.kwargs['override'])

    def test_url_is_rejected(self, load_dotenv):
        """
        test_url_is_rejected()
        ----------------------
        A pasted URL must fail with an actionable hostname error.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts URL validation.
        """
        settings = dict(DB_HOST='https://example.rds.amazonaws.com', DB_NAME='eoha',
                        DB_USER='postgres', DB_PASSWORD='secret')
        with patch.dict(os.environ, settings, clear=True):
            with self.assertRaisesRegex(ValueError, 'hostname'):
                database.connect_database()


class TestCheckDatabase(unittest.TestCase):
    """Existing tables must be readable and writable."""

    def test_read_only_user_is_rejected(self):
        """
        test_read_only_user_is_rejected()
        --------------------------------
        A login without write access cannot run ingestion.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts read-only credentials fail the preflight.
        """
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [('eoha', 'reader'), (False,)]
        with self.assertRaisesRegex(ValueError, 'INSERT and UPDATE'):
            database.check_database(connection)

    def test_wrong_database_is_rejected(self):
        """
        test_wrong_database_is_rejected()
        ---------------------------------
        A successful login to postgres must not pass the EOHA check.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts an actionable database mismatch error.
        """
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ('postgres', 'postgres')
        with self.assertRaisesRegex(ValueError, 'DB_NAME=eoha'):
            database.check_database(connection)

    def test_all_five_tables_pass(self):
        """
        test_all_five_tables_pass()
        --------------------------
        The new schema must pass without querying the legacy tables.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts identity and all five read-only table checks.
        """
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [('eoha', 'postgres')] + [(True,)] * 5
        self.assertEqual(database.check_database(connection), ('eoha', 'postgres'))
        queries = [call.args[0] for call in cursor.execute.call_args_list
                   if isinstance(call.args[0], database.sql.Composed)]
        self.assertEqual(len(queries), 5)
        for query in queries:
            self.assertNotIn('public.', str(query))
            self.assertIn('LIMIT 0', str(query))


class TestInitializeDatabase(unittest.TestCase):
    """The legacy schema must never be executed against the EOHA database."""

    def test_legacy_schema_is_not_executed(self):
        """
        test_legacy_schema_is_not_executed()
        --------------------------------------
        Reject old setup commands before they can create the wrong tables.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts the legacy schema is blocked without any SQL.
        """
        connection = MagicMock()
        with self.assertRaisesRegex(ValueError, 'without --init'):
            database.initialize_database(connection)
        connection.cursor.assert_not_called()


class TestPipelinePreflight(unittest.TestCase):
    """An unavailable database must prevent export submission."""

    def test_database_failure_prevents_google_initialization(self):
        """
        test_database_failure_prevents_google_initialization()
        ------------------------------------------------------
        Database errors must occur before any Google work or export costs.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts the database error propagates before EE initialization.
        """
        with patch.dict(os.environ, {'BUCKET_NAME': 'test-bucket'}, clear=True), \
             patch.object(pipeline, 'load_dotenv'), \
             patch.object(pipeline, 'connect_database', side_effect=ValueError('unreachable')), \
             patch.object(pipeline, 'create_google_clients') as google_clients:
                with self.assertRaisesRegex(ValueError, 'unreachable'):
                    pipeline.main()
        google_clients.assert_not_called()

    def test_missing_bucket_prevents_connections(self):
        """
        test_missing_bucket_prevents_connections()
        ------------------------------------------
        Missing storage configuration must fail before external connections.

        Parameters:
        -----------
        none

        Returns:
        --------
        void - asserts a clear bucket error and no database connection.
        """
        with patch.dict(os.environ, {}, clear=True), \
             patch.object(pipeline, 'load_dotenv'), \
             patch.object(pipeline, 'connect_database') as connect:
            with self.assertRaisesRegex(ValueError, 'BUCKET_NAME'):
                pipeline.main()
        connect.assert_not_called()


class TestPipelineTransforms(unittest.TestCase):
    """Earth Engine CSV rows must match the normalized EOHA schema."""

    def setUp(self):
        self.frame = pd.DataFrame([{
            'WardID': 29200001,
            'WardLabel': 'BUF_1',
            'Municipali': 'Buffalo City Metropolitan Municipality',
            'District': 'Buffalo City',
            'Province': 'Eastern Cape',
            'latitude': -32.9,
            'longitude': 27.8,
            'LST_Surface_Temp_C': 31.5,
            'Air_Temp_C': 22.1,
            'Soil_Moisture': 0.25,
            'NDVI_Vegetation_Index': 0.7,
            'NDWI_Water': 0.1,
            'Agric_Percentage': 0.2,
            'Population_Count': 15000,
            'Habitat_Class_Code': 40,
        }])

    def test_region_row_uses_ward_code(self):
        """One CSV row becomes one ward region with the expected identity."""
        rows = pipeline.build_region_rows(self.frame)
        self.assertEqual(rows, [
            ('29200001', 'BUF_1', 'Ward', 'Eastern Cape')
        ])

    def test_observation_rows_include_all_measurements(self):
        """One ward becomes eight normalized environmental observations."""
        rows = pipeline.build_observation_rows(self.frame, date(2026, 9, 1), 7)
        self.assertEqual(len(rows), 8)
        self.assertEqual({row[1] for row in rows}, {item[1] for item in pipeline.MEASUREMENTS})
        self.assertTrue(all(row[5] == 7 for row in rows))


if __name__ == '__main__':
    unittest.main()
