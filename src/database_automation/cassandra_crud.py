from typing import Any, Dict
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import subprocess
import time


class CassandraOperation:
    def __init__(self, contact_points: list, volume: str = "cassandra_data"):
        self.contact_points = contact_points
        self.schema = None
        self.volume = volume

        if not self._is_cassandra_running():
            started = self._start_cassandra_container()
            if not started:
                raise RuntimeError("Failed to start Cassandra Docker container.")
            
    def _is_cassandra_running(self):
        try:
            output = subprocess.check_output(["docker", "inspect", "-f", "{{.State.Running}}", "test-cassandra-v2"], stderr=subprocess.DEVNULL)
            return output.strip() == b"true"
        except subprocess.CalledProcessError:
            return False

    def _start_cassandra_container(self):
        try:
            # Check if the volume exists
            volume_exists = subprocess.run(["docker", "volume", "inspect", self.volume], stderr=subprocess.DEVNULL).returncode == 0
            # Create the volume if it doesn't exist
            if not volume_exists:
                subprocess.run(["docker", "volume", "create", self.volume], check=True)
                print(f"Docker volume {self.volume} created.")

            # Remove any existing container with the same name to avoid conflicts
            subprocess.run(["docker", "rm", "-f", "test-cassandra-v2"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            # Run the Cassandra container with the volume
            volume_path = f"{self.volume}:/var/lib/cassandra"
            container_id = subprocess.check_output([
                "docker", "run", "--name", "test-cassandra-v2",
                "-p", "9042:9042",
                "-v", volume_path,
                "-d", "cassandra:latest"
            ], stderr=subprocess.DEVNULL)
            
            print(f"Container ID created: {container_id.decode().strip()}")
            for _ in range(10):  # Retry up to n times 
                if self._is_cassandra_running():
                    print("Cassandra is running.")
                    return True
                time.sleep(2)  # Check every 2 seconds
            print("Error: Cassandra did not start in the expected time.")
            return False
        except subprocess.CalledProcessError as e:
            print(f"Error starting Cassandra container: {e}")
            return False

    def connect(self, username=None, password=None):
        self.__username = username
        self.__password = password
        start_time = time.time()
        timeout = 120
        auth_provider = None
        message_printed = False
        if self.__username and self.__password:
            auth_provider = PlainTextAuthProvider(username=self.__username, password=self.__password)
        while time.time() - start_time < timeout:
            try:
                self.__cluster = Cluster(self.contact_points, auth_provider=auth_provider, port=9042)
                self.__session = self.__cluster.connect()
                print("Cassandra is ready.")
                return self.__session
            except Exception as e:
                if not message_printed and 'ConnectionResetError' in str(e):
                    print(f"Error connecting: {e}")
                    print(f"It seems Cassandra isn't ready yet. Give us a minute while we ensure it is fully operational..")
                message_printed = True
                time.sleep(5)  # Wait for 5 seconds before retrying
        raise RuntimeError("Cassandra did not become ready in the expected time.")

    def create_keyspace(self, keyspace_name: str, strategy: str = 'SimpleStrategy', replicas: int = 1):
        self.__session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': '{strategy}', 'replication_factor' : {replicas}}};")

    def use_keyspace(self, keyspace_name: str):
        self.__session.set_keyspace(keyspace_name)

    def create_table(self, table_name: str, schema: Dict[str, str]):
        if self.__session.keyspace:
            table = self.__session.keyspace + '.' +  table_name
            schema_str = ', '.join([f"{column} {datatype}" for column, datatype in schema.items()])
            self.__session.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema_str});")
        else:
            raise ValueError("No keyspace selected in the current session.") 

    def get_table_schema(self, table_name: str):
        keyspace_name = self.__session.keyspace
        if keyspace_name is None:
            raise ValueError("No keyspace selected in the current session.")
        metadata = self.__session.cluster.metadata
        keyspace = metadata.keyspaces.get(keyspace_name)
        if keyspace is None:
            raise ValueError(f"Keyspace '{keyspace_name}' not found in metadata.")

        table = keyspace.tables.get(table_name)

        if table is None:
            raise ValueError(f"Table '{table_name}' not found in keyspace '{keyspace_name}'.")

        schema = {column.name: str(column.cql_type) for column in table.columns.values()}
        return schema

    def switch_keyspace(self, keyspace_name: str):
        if keyspace_name in self.__cluster.metadata.keyspaces:
            self.__session.set_keyspace(keyspace_name)
            self.keyspace = keyspace_name
            print(f"Switched to keyspace: {keyspace_name}")
        else:
            raise ValueError(f"Keyspace '{keyspace_name}' does not exist.")

    def insert_record(self, table_name: str, record: Dict):
        table = self.__session.keyspace + '.' + table_name
        columns = ', '.join(record.keys())
        values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in record.values()])
        query = f"INSERT INTO {table} ({columns}) VALUES ({values});"
        self.__session.execute(query)

    def bulk_insert(self, datafile: str, table_name: str):
        if datafile.endswith('.csv'):
            dataframe = pd.read_csv(datafile, encoding='utf-8')
        elif datafile.endswith(".xlsx"):
            dataframe = pd.read_excel(datafile, encoding='utf-8')  
        for _, row in dataframe.iterrows():
            record = row.to_dict()
            print(record)
            self.insert_record(table_name, record)

    def fetch_records(self, table_name: str):
        query = f"SELECT * FROM {table_name};"
        rows = self.__session.execute(query)
        return rows

    def update_record(self, table_name: str, condition_column: str, condition_value: Any, update_values: Dict):
        schema = self.get_table_schema(table_name)

        # Check condition value format
        if condition_column not in schema:
            raise ValueError(f"Condition column '{condition_column}' not found in table schema.")

        if not self._is_value_valid(schema[condition_column], condition_value):
            raise ValueError(f"Condition value '{condition_value}' does not match schema type '{schema[condition_column]}'.")

        # Check update values format
        for key, value in update_values.items():
            if key not in schema:
                raise ValueError(f"Update column '{key}' not found in table schema.")

            if not self._is_value_valid(schema[key], value):
                raise ValueError(f"Update value '{value}' for column '{key}' does not match schema type '{schema[key]}'.")

        # Construct update query
        set_values = ', '.join([f"{key} = '{value}'" if isinstance(value, str) else f"{key} = {value}" for key, value in update_values.items()])
        if isinstance(condition_value, str):
            condition_value_str = f"'{condition_value}'"
        else:
            condition_value_str = str(condition_value)

        query = f"UPDATE {table_name} SET {set_values} WHERE {condition_column} = {condition_value_str};"
        self.__session.execute(query)

    def delete_record(self, table_name: str, condition_column: str, condition_value: Any):
        query = f"DELETE FROM {table_name} WHERE {condition_column} = '{condition_value}';"
        self.__session.execute(query)

    def _is_value_valid(self, expected_type: str, value: Any) -> bool:
        # Implement your validation logic here based on expected_type and value
        # Example: Check if value matches expected_type
        return isinstance(value, str) if expected_type == 'text' else isinstance(value, int) if expected_type == 'int' else False

    def close(self):
        if self.__session:
            self.__session.shutdown()
        self.stop_container()

    def stop_container(self):
        try:
            subprocess.run(["docker", "stop", "test-cassandra-v2"], check=True)
            subprocess.run(["docker", "rm", "test-cassandra-v2"], check=True)
            print("Cassandra container stopped and removed.")
        except subprocess.CalledProcessError as e:
            print(f"Error stopping Cassandra container: {e}")
