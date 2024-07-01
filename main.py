from pymongo import MongoClient

# Replace the following with your MongoDB connection string
connection_string = "mongodb://username:password@host:port/database_name"

# Create a MongoClient to the running mongod instance
client = MongoClient(connection_string)

# Specify the database
db = client['database_name']

# Specify the collection
collection = db['collection_name']

# Example: Find one document in the collection
document = collection.find_one()
print(document)

# Example: Insert a document into the collection
new_document = {"name": "John Doe", "age": 30}
collection.insert_one(new_document)
