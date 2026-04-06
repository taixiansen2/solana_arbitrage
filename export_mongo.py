import os
import json
from pymongo import MongoClient
from bson.json_util import dumps

def main():
    # Connect to MongoDB
    mongo_uri = os.environ.get("MONGODB_URI", "mongodb://127.0.0.1:27017")
    client = MongoClient(mongo_uri)
    db = client[os.environ.get("MONGODB_DB", "propamm")]
    collection = db[os.environ.get("MONGODB_COLLECTION", "tx_summaries")]
    
    total_docs = collection.count_documents({})
    print(f"Total documents to export: {total_docs}")
    
    chunk_size = 10000
    cursor = collection.find({}).batch_size(chunk_size)
    
    part = 1
    current_chunk = []
    
    for doc in cursor:
        current_chunk.append(doc)
        if len(current_chunk) >= chunk_size:
            filename = f"/out/export_data_part{part}.json"
            with open(filename, "w", encoding="utf-8") as f:
                # Use bson.json_util.dumps to handle MongoDB specific types like ObjectId
                f.write(dumps(current_chunk, indent=2))
            print(f"Wrote {len(current_chunk)} records to {filename}")
            part += 1
            current_chunk = []
            
    # Write remaining documents
    if current_chunk:
        filename = f"/out/export_data_part{part}.json"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(dumps(current_chunk, indent=2))
        print(f"Wrote {len(current_chunk)} records to {filename}")

if __name__ == "__main__":
    main()
