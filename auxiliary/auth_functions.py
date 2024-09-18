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


def fetch_collection(db_name: str = None,
                     collection_name: str = None,
                     is_dev: bool = False,
                     query: dict = {}):
    with open('dicts/auth.json') as config_file:
        config = json.load(config_file)
        config_file.close()
    connection_uri = config.get('mongo_connection_string')
    client = MongoClient(connection_uri)
    db = client[db_name]
    collection = db[collection_name]
    if is_dev:
        return list(collection.find(query))
    else:
        return list(collection.find({"bitstream": {"$ne": ""}}))
