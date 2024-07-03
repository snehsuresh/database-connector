import mysql.connector
from mysql.connector import errorcode
from typing import Optional, Any, List, Dict, Union

class MySQLConnection:
    def __init__(self, host: str, user: str, password: str, database: str = None, port: int = 3306):
        self.__host = host
        self.__user = user
        self.__password = password
        self.__database = database
        self.__port = port
        self.connection: Optional[Any] = None
        self.__cursor: Optional[Any] = None

    def connect(self) -> None:
        try:
            self.__connection = mysql.connector.connect(
                host=self.__host,
                user=self.__user,
                password=self.__password,
                database=self.__database if self.__database else None,
                port=self.__port
            )
            self.__cursor = self.__connection.cursor()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR and self.__database:
                self.create_database()
                self.__connection = mysql.connector.connect(
                    host=self.__host,
                    user=self.__user,
                    password=self.__password,
                    database=self.__database,
                    port=self.__port
                )
                self.__cursor = self.__connection.cursor()
            else:
                raise Exception(f"Error connecting to the database: {err}")

    def disconnect(self) -> None:
        try:
            if self.__connection and self.__connection.is_connected():
                if self.__cursor:
                    self.__cursor.close()
                if self.__connection:
                    self.__connection.close() # type: ignore
        except mysql.connector.Error as err:
            raise Exception(f"Error disconnecting from the database: {err}")

    def create_database(self) -> None:
        try:
            temp_connection = mysql.connector.connect(
                host=self.__host,
                user=self.__user,
                password=self.__password,
                port=self.__port
            )
            temp_cursor = temp_connection.cursor()
            if temp_cursor:
                temp_cursor.execute(f"CREATE DATABASE {self.__database}")
                temp_cursor.close()
            if temp_connection:
                temp_connection.close() # type: ignore
        except mysql.connector.Error as err:
            raise Exception(f"Failed to create database: {err}")

    def create_table(self, table_name: str, columns: Dict[str, str]) -> None:
        try:
            self.connect()
            if self.__cursor and self.__connection:
                columns_str = ', '.join([f'{col} {data_type}' for col, data_type in columns.items()])
                create_table_query = f'CREATE TABLE {table_name} ({columns_str})'
                self.__cursor.execute(create_table_query)
                self.__connection.commit()
        except mysql.connector.Error as err:
            raise Exception(f"Failed to create table: {err}")
        finally:
            self.disconnect()

    def insert_record(self, table_name: str, record: Dict[str, Any]) -> None:
        try:
            self.connect()
            if self.__cursor and self.__connection:
                columns = ', '.join(record.keys())
                values = tuple(record.values())
                placeholders = ', '.join(['%s'] * len(record))
                insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
                self.__cursor.execute(insert_query, values)
                self.__connection.commit()
        except mysql.connector.Error as err:
            raise Exception(f"Failed to insert record: {err}")
        finally:
            self.disconnect()

    def select_record(self, table_name: str, conditions: str = None) -> List[List[Any]]:
        try:
            self.connect()
            if self.__cursor:
                select_query = f'SELECT * FROM {table_name}'
                if conditions:
                    select_query += f' WHERE {conditions}'
                self.__cursor.execute(select_query)
                records = self.__cursor.fetchall()
                return [list(record) for record in records]  # Convert tuple to list
            return []
        except mysql.connector.Error as err:
            raise Exception(f"Failed to select record: {err}")
        finally:
            self.disconnect()

    def update_record(self, table_name: str, record: Dict[str, Any], conditions: str) -> None:
        try:
            self.connect()
            if self.__cursor and self.__connection:
                set_clause = ', '.join([f'{key}=%s' for key in record.keys()])
                values = tuple(record.values())
                update_query = f'UPDATE {table_name} SET {set_clause} WHERE {conditions}'
                self.__cursor.execute(update_query, values)
                self.__connection.commit()
        except mysql.connector.Error as err:
            raise Exception(f"Failed to update record: {err}")
        finally:
            self.disconnect()

    def delete_record(self, table_name: str, conditions: str) -> None:
        try:
            self.connect()
            if self.__cursor and self.__connection:
                delete_query = f'DELETE FROM {table_name} WHERE {conditions}'
                self.__cursor.execute(delete_query)
                self.__connection.commit()
        except mysql.connector.Error as err:
            raise Exception(f"Failed to delete record: {err}")
        finally:
            self.disconnect()
