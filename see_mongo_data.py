import os

from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables from .env file (if you're using one)
load_dotenv()

# MongoDB connection settings
MONGO_URI = "mongodb+srv://mqvist:hHvSyIdnhyPyodDQ@cluster0.xh2f6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DB_NAME = "job_scraping"


try:
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    # List all collections in the database
    print("Collections in the database:")
    collections = db.list_collection_names()
    for collection in collections:
        print(f"- {collection}")

    # Specify a collection to view data from (replace 'your_collection' with actual collection name)
    collection_name = input(
        "\nEnter the collection name to view data (or press Enter to skip): "
    ).strip()
    if collection_name in collections:
        collection = db[collection_name]

        # Retrieve and display up to 5 documents from the collection
        print(f"\nData in collection '{collection_name}' (up to 5 documents):")
        documents = collection.find().limit(5)  # Limit to 5 for brevity
        doc_count = collection.count_documents({})  # Total document count
        print(f"Total documents in '{collection_name}': {doc_count}\n")

        for doc in documents:
            # Remove '_id' for cleaner output (optional)
            doc.pop("_id", None)
            print(doc)
    elif collection_name:
        print(f"Collection '{collection_name}' does not exist.")
    else:
        print("No collection selected.")

    # Close the connection
    client.close()

except Exception as e:
    print(f"Error connecting to MongoDB or retrieving data: {e}")
