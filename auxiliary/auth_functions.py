# -*- coding: utf-8 -*-
"""
Created on Wed 26 June 2024

@author: AfricaMultiple (NTViswajith)
"""
# Libraries
from pymongo import MongoClient

# Fetches specified collection's data & returns json objects list

# Fill in the MongoDB Client bot URI (connection_uri) below
def fetch_collection(connection_uri="",
                     db_name=None,
                     collection_name=None):
    client = MongoClient(connection_uri)
    db = client[db_name]
    collection = db[collection_name]
    return list(collection.find({"bitstream": {"$ne": ""}}))

