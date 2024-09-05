# -*- coding: utf-8 -*-
"""
Created on Wed 26 June 2024

@author: AfricaMultiple (NTViswajith)
"""
# Libraries
import json
from pymongo import MongoClient

# Fetches specified collection's data & returns json objects list

# Fill in the auth_functions_config.json file for MongoDB Client bot URI
def fetch_collection(db_name=None, collection_name=None):
    with open('auxiliary/auth_functions_config.json') as config_file:
        config = json.load(config_file)
    connection_uri = config.get('connection_uri', '')
    client = MongoClient(connection_uri)
    db = client[db_name]
    collection = db[collection_name]
    return list(collection.find({"bitstream": {"$ne": ""}}))

