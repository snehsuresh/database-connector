import pytest
import mysql.connector
from mysql.connector import errorcode
from database_automation.mysql_crud import MySQLConnection 
from unittest.mock import MagicMock, patch

# Test configuration
TEST_DB_NAME = 'test_db'
TEST_TABLE_NAME = 'test_table'

# Helper function to create a new MySQLConnection instance
def create_test_db_connection():
    return MySQLConnection(
        host='localhost',
        user='root',  # Replace with your MySQL user
        password='12345678',  
        database=TEST_DB_NAME
    )

@pytest.fixture(scope='module', autouse=True)
def setup_and_teardown():
    # Setup: create a test database and table
    conn = create_test_db_connection()
    try:
        conn.create_database()
        conn.create_table(TEST_TABLE_NAME, {
            'id': 'INT AUTO_INCREMENT PRIMARY KEY',
            'name': 'VARCHAR(255)',
            'value': 'INT'
        })
        yield
    finally:
        # Teardown: drop the test database
        conn.disconnect()
        conn = create_test_db_connection()
        try:
            temp_conn = mysql.connector.connect(
                host='localhost',
                user='root',
                password='12345678'
            )
            temp_cursor = temp_conn.cursor()
            temp_cursor.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}")
            temp_cursor.close()
            temp_conn.close()
        except mysql.connector.Error as err:
            pytest.fail(f"Teardown failed: {err}")

def test_create_database():
    with patch('database_automation.mysql_crud.mysql.connector.connect') as mock_connect:
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        db_conn = create_test_db_connection()
        db_conn.create_database()

        mock_cursor.execute.assert_called_once_with(f"CREATE DATABASE IF NOT EXISTS {TEST_DB_NAME}")

def test_insert_record():
    conn = create_test_db_connection()
    record = {'name': 'Test', 'value': 123}
    conn.insert_record(TEST_TABLE_NAME, record)
    result = conn.select_record(TEST_TABLE_NAME, "name='Test'")
    assert len(result) == 1
    assert result[0] == (1, 'Test', 123)

def test_update_record():
    conn = create_test_db_connection()
    conn.delete_record(TEST_TABLE_NAME, "name='Test'") #cleanup
    record = {'name': 'Test', 'value': 123}
    conn.insert_record(TEST_TABLE_NAME, record)

    updated_record = {'value': 456}
    conn.update_record(TEST_TABLE_NAME, updated_record, "name='Test'")
    
    result = conn.select_record(TEST_TABLE_NAME, "name='Test'")
    
    assert len(result) == 1, f"Expected 1 record, found {len(result)}: {result}"

def test_delete_record():
    conn = create_test_db_connection()
    record = {'name': 'Test', 'value': 123}
    conn.insert_record(TEST_TABLE_NAME, record)
    conn.delete_record(TEST_TABLE_NAME, "name='Test'")
    result = conn.select_record(TEST_TABLE_NAME, "name='Test'")
    assert len(result) == 0
