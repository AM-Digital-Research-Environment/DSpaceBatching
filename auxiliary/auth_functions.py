# -*- coding: utf-8 -*-
"""
Created on Wed 26 June 2024

@author: AfricaMultiple (NTViswajith)
"""
# Libraries
import json
import pathlib
import sys

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

CONFIG_PATH = pathlib.Path('auxiliary/auth_functions_config.json')


# Fetches specified collection's data & returns json objects list

# Fill in the auth_functions_config.json file for MongoDB Client bot URI
def fetch_collection(db_name=None, collection_name=None):
    # Check config file exists
    if not CONFIG_PATH.exists():
        print(
            "[ERROR] Config file not found: auxiliary/auth_functions_config.json\n"
            "  Create the file with the following format:\n"
            '  {"mongo_uri": "mongodb://<user>:<password>@<host>:<port>/?authMechanism=DEFAULT"}'
        )
        sys.exit(1)

    with CONFIG_PATH.open() as config_file:
        config = json.load(config_file)

    # Check URI is present
    connection_uri = config.get('mongo_uri', '').strip()
    if not connection_uri:
        print(
            "[ERROR] 'mongo_uri' is empty in auxiliary/auth_functions_config.json\n"
            "  Please fill in your MongoDB connection URI."
        )
        sys.exit(1)

    # Try to connect (short timeout to detect VPN / network issues quickly)
    try:
        client = MongoClient(connection_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
    except (ConnectionFailure, ServerSelectionTimeoutError):
        print(
            "[ERROR] Cannot connect to MongoDB.\n"
            "  Possible causes:\n"
            "  - You are not connected to the VPN\n"
            "  - The MongoDB server is unreachable\n"
            "  - The URI in auth_functions_config.json is incorrect\n"
            f"  Host: {connection_uri.split('@')[-1].split('/')[0] if '@' in connection_uri else '(unknown)'}"
        )
        sys.exit(1)

    db = client[db_name]
    collection = db[collection_name]
    return list(collection.find({"bitstream": {"$ne": ""}}))

