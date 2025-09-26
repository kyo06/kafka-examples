"""
Author: Mohammed REZGUI
Version: 1.0.0
Description: Unit tests for the KSQLDB example.
"""

import unittest
from unittest.mock import patch, MagicMock
import json

class TestKSQLDBClient(unittest.TestCase):

    def setUp(self):
        self.ksqldb_url = "http://localhost:8088"

    @patch('main.requests.Session')
    def test_client_initialization(self, mock_session_class):
        """Test client initialization"""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        from main import KSQLDBClient
        client = KSQLDBClient(self.ksqldb_url)

        self.assertEqual(client.base_url, self.ksqldb_url)
        self.assertEqual(client.session, mock_session)

    @patch('main.requests.Session')
    def test_execute_statement(self, mock_session_class):
        """Test executing KSQL statement"""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        from main import KSQLDBClient
        client = KSQLDBClient(self.ksqldb_url)

        sql = "CREATE STREAM test_stream (id VARCHAR) WITH (KAFKA_TOPIC='test');"
        result = client.execute_statement(sql)

        # Verify POST was called with correct payload
        expected_url = f"{self.ksqldb_url}/ksql"
        expected_payload = {
            "ksql": sql,
            "streamsProperties": {}
        }
        mock_session.post.assert_called_once_with(expected_url, json=expected_payload)

        self.assertEqual(result, {"status": "success"})

    @patch('main.requests.Session')
    def test_query_stream(self, mock_session_class):
        """Test streaming query"""
        mock_session = MagicMock()
        mock_response = MagicMock()
        # Mock streaming response
        mock_response.iter_lines.return_value = [
            b'{"row":{"columns":["value1", "value2"]}}',
            b'{"row":{"columns":["value3", "value4"]}}'
        ]
        mock_response.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        from main import KSQLDBClient
        client = KSQLDBClient(self.ksqldb_url)

        sql = "SELECT * FROM test_stream EMIT CHANGES;"
        results = list(client.query_stream(sql))

        expected_url = f"{self.ksqldb_url}/query-stream"
        expected_payload = {
            "sql": sql,
            "properties": {
                "auto.offset.reset": "earliest"
            }
        }
        mock_session.post.assert_called_once_with(
            expected_url,
            json=expected_payload,
            stream=True,
            headers={'Accept': 'application/vnd.ksqlapi.delimited.v1'}
        )

        expected_results = [
            {"row": {"columns": ["value1", "value2"]}},
            {"row": {"columns": ["value3", "value4"]}}
        ]
        self.assertEqual(results, expected_results)

    @patch('main.requests.Session')
    def test_insert_into_stream(self, mock_session_class):
        """Test inserting data into stream"""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "inserted"}
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        from main import KSQLDBClient
        client = KSQLDBClient(self.ksqldb_url)

        stream_name = "test_stream"
        values = "( 'id1', 'data1' )"
        result = client.insert_into_stream(stream_name, values)

        expected_sql = f"INSERT INTO {stream_name} VALUES {values};"
        expected_url = f"{self.ksqldb_url}/ksql"
        expected_payload = {
            "ksql": expected_sql,
            "streamsProperties": {}
        }
        mock_session.post.assert_called_once_with(expected_url, json=expected_payload)

        self.assertEqual(result, {"status": "inserted"})

    @patch('main.requests.Session')
    def test_list_streams(self, mock_session_class):
        """Test listing streams"""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"streams": ["stream1", "stream2"]}
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        from main import KSQLDBClient
        client = KSQLDBClient(self.ksqldb_url)

        result = client.list_streams()

        expected_sql = "SHOW STREAMS;"
        expected_url = f"{self.ksqldb_url}/ksql"
        expected_payload = {
            "ksql": expected_sql,
            "streamsProperties": {}
        }
        mock_session.post.assert_called_once_with(expected_url, json=expected_payload)

        self.assertEqual(result, {"streams": ["stream1", "stream2"]})

    @patch('main.requests.Session')
    def test_list_tables(self, mock_session_class):
        """Test listing tables"""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"tables": ["table1", "table2"]}
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        from main import KSQLDBClient
        client = KSQLDBClient(self.ksqldb_url)

        result = client.list_tables()

        expected_sql = "SHOW TABLES;"
        expected_url = f"{self.ksqldb_url}/ksql"
        expected_payload = {
            "ksql": expected_sql,
            "streamsProperties": {}
        }
        mock_session.post.assert_called_once_with(expected_url, json=expected_payload)

        self.assertEqual(result, {"tables": ["table1", "table2"]})

    @patch('main.requests.Session')
    @patch('builtins.print')
    def test_main_execution(self, mock_print, mock_session_class):
        """Test main execution flow"""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        # Mock query_stream to return empty generator
        with patch.object(KSQLDBClient, 'query_stream', return_value=iter([])):
            import main

            # The main code creates stream, inserts, and queries
            # Since query_stream is mocked to empty, it should complete

if __name__ == '__main__':
    unittest.main()