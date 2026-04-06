import os
import sqlite3
from pymongo import MongoClient

def main():
    mongo_uri = os.environ.get("MONGODB_URI", "mongodb://mongo:27017")
    client = MongoClient(mongo_uri)
    db = client[os.environ.get("MONGODB_DB", "propamm")]
    coll = db[os.environ.get("MONGODB_COLLECTION", "tx_summaries")]
    
    cnt = coll.count_documents({'slot': {'$gte': 407000000, '$lte': 407001000}})
    print(f"Mongo count [407000000, 407001000]: {cnt}")
    
    cnt_lt = coll.count_documents({'slot': {'$lt': 407000000}})
    print(f"Mongo count < 407000000: {cnt_lt}")
    
    cnt_gt = coll.count_documents({'slot': {'$gt': 407001000}})
    print(f"Mongo count > 407001000: {cnt_gt}")

    print("Sample slot:", coll.find_one({}, {"slot": 1}))
    
if __name__ == "__main__":
    main()