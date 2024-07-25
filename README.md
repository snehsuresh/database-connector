# dbLinkPro

`dbLinkPro` is a versatile Python package designed to simplify database interactions by offering a unified interface for performing CRUD (Create, Read, Update, Delete) operations across Cassandra, MongoDB, and MySQL databases. This package not only supports essential database operations but also enhances the user experience with additional features specific to each database type.

## Features

### Cassandra-Specific Enhancements

- **Docker Integration**: Automatically starts a Cassandra Docker container if it's not running. Handles container creation, volume management, and startup checks.
- **Connection Timeout Handling**: Implements retry logic to handle connection delays, ensuring that Cassandra is fully operational before proceeding.
- **Keyspace and Table Management**: Create and manage keyspaces and tables with flexible schema definitions. Switch between keyspaces easily.
- **Schema Retrieval**: Retrieve and validate table schemas, including checking the compatibility of data types for updates and deletions.
- **Bulk Data Ingestion**: Supports bulk insertion of data from CSV and Excel files into Cassandra tables.
- **Custom Query Handling**: Construct and execute custom queries for inserting, updating, and deleting records with validation based on schema types.
- **Container Lifecycle Management**: Start, stop, and remove Cassandra Docker containers programmatically.

## Installation

You can install `dbLinkPro` using pip:

```bash
pip install dbLinkPro
```

## Usage

### MySQL
```bash
from database_automation import mysql_crud
```

Create a connection instance

```bash
connection = mysql_crud.MySQLConnection(
    host='localhost',
    user='your_username',
    password='your_password',
    database='test_db',
    port=3306
)
```

Connect to the database
```bash
connection.connect()
```

Create a new database if it does not exist
```bash
connection.create_database()
```

Table operations
```bash
columns = {
    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
    'name': 'VARCHAR(100)',
    'age': 'INT'
}
connection.create_table('person', columns)

record = {
    'name': 'Alice',
    'age': 28
}
connection.insert_record('person', record)

records = connection.select_record('person')
print(records)  # Output: [[1, 'Alice', 28]]

update_values = {'age': 29}
connection.update_record('person', update_values, 'name = "Alice"')

connection.delete_record('person', 'name = "Alice"')
```

Disconnect from the database
```bash
connection.disconnect()
```

### MongoDB

```bash
from database_automation import mongo_crud
```
Create an instance of MongoOperation
```bash
mongo = mongo_crud.MongoOperation(
    client_url='mongodb://localhost:27017/',
    database_name='test_db',
    collection_name='test_collection'
)
```
Create a MongoDB client
```bash
client = mongo.create_mongo_client()
```
Create or access the database
```bash
database = mongo.create_database()
```
Create or access the collection
```bash
collection = mongo.create_collection()
```

CRUD operations
```bash
single_record = {'name': 'John Doe', 'age': 35}
mongo.insert_record(single_record, 'test_collection')

multiple_records = [
    {'name': 'Jane Smith', 'age': 28},
    {'name': 'Emily Davis', 'age': 40}
]
mongo.insert_record(multiple_records, 'test_collection')

mongo.bulk_insert('data.csv', 'test_collection')

mongo.bulk_insert('data.xlsx', 'test_collection')

```

### Cassandra

```bash
from database_automation import cassandra_crud
```

Create an instance of CassandraOperation
```bash
cassandra = cassandra_crud.CassandraOperation(
    contact_points=['127.0.0.1'],
    volume='cassandra_data'
)
```

Connect to Cassandra

```bash
session = cassandra.connect(username='your_username', password='your_password')
```

Create a keyspace
```bash
cassandra.create_keyspace('test_keyspace', strategy='SimpleStrategy', replicas=1)
```

Use the created keyspace
```bash
cassandra.use_keyspace('test_keyspace')
```

Define table schema
```bash
schema = {
    'id': 'int',
    'name': 'text',
    'age': 'int'
    }
```

Create a table
```bash
cassandra.create_table('test_table', schema)
```

Insert a record
```bash
record = {'id': 1, 'name': 'John Doe', 'age': 30}
cassandra.insert_record('test_table', record)
```

Bulk insert from a CSV file
```bash
cassandra.bulk_insert('data.csv', 'test_table')
```

Fetch Records
```bash
rows = cassandra.fetch_records('test_table')
for row in rows:
    print(row)
```

Update a record
```bash
update_values = {'name': 'Jane Doe', 'age': 29}
cassandra.update_record('test_table', 'id', 1, update_values)
```

Delete a record
```bash
cassandra.delete_record('test_table', 'id', 1)
```

Close the connection
```bash
cassandra.close()
```

## Contributing
I welcome contributions to dbLinkPro. If you'd like to contribute, please..

1. Fork the repository on GitHub.
2. Create a new branch for your changes.
3. Make your changes and commit them with descriptive messages.
4. Push your changes to your fork.
5. Submit a pull request with a detailed explanation of your changes.

## License
dbLinkPro is licensed under the MIT License.

## Author/Maintainer
Sneh Pillai

## Contact Information
For support or questions, please contact: snehpillai02@gmail.com

