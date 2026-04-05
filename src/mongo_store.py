"""Shared MongoDB connection for fetch + analyze."""

from __future__ import annotations

import os

from pymongo import MongoClient
from pymongo.collection import Collection


def get_tx_collection(*, create_index: bool = False) -> tuple[MongoClient, Collection]:
    uri = os.environ.get("MONGODB_URI", "mongodb://mongo:27017")
    db_name = os.environ.get("MONGODB_DB", "propamm")
    coll_name = os.environ.get("MONGODB_COLLECTION", "tx_summaries")
    client = MongoClient(uri, serverSelectionTimeoutMS=30_000)
    client.admin.command("ping")
    coll = client[db_name][coll_name]
    if create_index:
        coll.create_index("signature", unique=True)
    return client, coll
