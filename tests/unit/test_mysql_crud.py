import unittest
from unittest.mock import patch, MagicMock
from database_automation.mysql_crud import MySQLConnection
from mysql.connector import errorcode
import mysql.connector


class TestMySQLConnection(unittest.TestCase):

    def setUp(self):
        self.host = '127.0.0.1'
        self.user = 'root'
        self.password = 'password'
        self.database = 'test_db'
        self.port = 3306
        self.db_conn = MySQLConnection(
            host=self.host, user=self.user, password=self.password,
            database=self.database, port=self.port
        )

    @patch('database_automation.mysql_crud.mysql.connector.connect')
    def test_connect_success(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        self.db_conn.connect()

        mock_connect.assert_called_once_with(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port
        )
        self.assertEqual(self.db_conn._MySQLConnection__connection, mock_connection)
        self.assertEqual(self.db_conn._MySQLConnection__cursor, mock_cursor)

    @patch('database_automation.mysql_crud.mysql.connector.connect')
    def test_connect_database_creation(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.side_effect = [
            mysql.connector.Error(errno=errorcode.ER_BAD_DB_ERROR),
            mock_connection
        ]
        mock_connection.cursor.return_value = mock_cursor

        with patch.object(self.db_conn, 'create_database') as mock_create_db:
            self.db_conn.connect()

            mock_create_db.assert_called_once()
            mock_connect.assert_called_with(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port
            )

    @patch('database_automation.mysql_crud.mysql.connector.connect')
    def test_connect_failure(self, mock_connect):
        mock_connect.side_effect = mysql.connector.Error("Connection error")

        with self.assertRaises(Exception) as context:
            self.db_conn.connect()
        self.assertTrue('Error connecting to the database' in str(context.exception))

    @patch('database_automation.mysql_crud.mysql.connector.connect')
    def test_disconnect_success(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.is_connected.return_value = True
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        self.db_conn.connect()
        self.db_conn.disconnect()

        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch('database_automation.mysql_crud.mysql.connector.connect')
    def test_disconnect_failure(self, mock_connect):
        mock_connection = MagicMock()
        mock_connection.is_connected.return_value = True
        mock_connection.close.side_effect = mysql.connector.Error("Disconnection error")
        mock_connect.return_value = mock_connection
        self.db_conn.connect()

        with self.assertRaises(Exception) as context:
            self.db_conn.disconnect()
        self.assertTrue('Error disconnecting from the database' in str(context.exception))

    @patch('database_automation.mysql_crud.mysql.connector.connect')
    def test_create_database(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        self.db_conn.create_database()

        mock_cursor.execute.assert_called_once_with(f"CREATE DATABASE {self.database}")
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch('database_automation.mysql_crud.mysql.connector.connect')
    def test_create_database_failure(self, mock_connect):
        mock_connect.side_effect = mysql.connector.Error("Database creation error")

        with self.assertRaises(Exception) as context:
            self.db_conn.create_database()
        self.assertTrue('Failed to create database' in str(context.exception))

    @patch('database_automation.mysql_crud.mysql.connector.connect')
    def test_create_table(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        self.db_conn.create_table('test_table', {'id': 'INT', 'name': 'VARCHAR(255)'})

        mock_cursor.execute.assert_called_once_with(
            'CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR(255))'
        )
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch('database_automation.mysql_crud.mysql.connector.connect')
    def test_insert_record(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        self.db_conn.insert_record('test_table', {'id': 1, 'name': 'John'})

        mock_cursor.execute.assert_called_once_with(
            'INSERT INTO test_table (id, name) VALUES (%s, %s)',
            (1, 'John')
        )
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch('database_automation.mysql_crud.mysql.connector.connect')
    def test_select_record(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, 'John')]

        result = self.db_conn.select_record('test_table')

        mock_cursor.execute.assert_called_once_with('SELECT * FROM test_table')
        self.assertEqual(result, [(1, 'John')])
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch('database_automation.mysql_crud.mysql.connector.connect')
    def test_update_record(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        self.db_conn.update_record('test_table', {'name': 'Doe'}, 'id=1')

        mock_cursor.execute.assert_called_once_with(
            'UPDATE test_table SET name=%s WHERE id=1',
            ('Doe',)
        )
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch('database_automation.mysql_crud.mysql.connector.connect')
    def test_delete_record(self, mock_connect):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        self.db_conn.delete_record('test_table', 'id=1')

        mock_cursor.execute.assert_called_once_with(
            'DELETE FROM test_table WHERE id=1'
        )
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
