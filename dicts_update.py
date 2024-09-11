"""
This script file contains a class which allows for the updating the dictionary files that are use in the batcher script,
these include the following dictionaries:
- Licenses
- Projects
- Institutions
"""

# Libraries
import requests
from types import NoneType
import jmespath as jp
from pymongo import MongoClient
from auxiliary.helper_functions import json_file


class Dictionary:

    def __init__(self, dict_name: list | str = "all"):
        self._auth_dict = json_file("dicts/auth.json")
        self._json_query = '_embedded.searchResult._embedded.objects[]._embedded[].indexableObject[]'
        self._mongo_database = MongoClient(self._auth_dict['mongo_connection_string'])['dspace_metadata_ubt']
        self._params = {
            "license": {
                "query": "f.entityType=License,equals&page=0&size=1000",
                "mongo_collection": "licenses"
            },
            "fundingAgency": {
                "query": "f.entityType=FundingAgency,equals&page=0&size=1000",
                "mongo_collection": "fundingAgencies"
            },
            "university": {
                "query": "f.entityType=University,equals&page=0&size=1000",
                "mongo_collection": "universities"
            },
            "faculty": {
                "query": "f.entityType=Faculty,equals&page=0&size=1000",
                "mongo_collection": "faculties"
            },
            "department": {
                "query": "f.entityType=Department,equals&page=0&size=1000",
                "mongo_collection": "departments"
            }
        }
        self._dict = list(self._params.keys()) if dict_name == "all" else [dict_name]

    def api_response(self, preview: bool = True, entity: str | NoneType = None):
        if entity:
            _url = self._auth_dict['api_endpoint'] + self._params.get(entity).get('query')
            _payload = {}
            _headers = {
                'Authorization': self._auth_dict['api_bearer_token'],
                'Cookie': self._auth_dict['api_cookie']
            }
            response = jp.search(self._json_query,
                                 requests.request("GET", _url, headers=_headers, data=_payload).json())
            if preview:
                print(response)
            else:
                return response
        else:
            print(
                "Please provide the entity type for preview. The following are list of choices:\n" +
                "\n".join(list(self._params.keys()))
            )

    def update(self):
        for entity in self._dict:
            mongo_collection = self._mongo_database[self._params.get(entity).get('mongo_collection')]
            for doc in self.api_response(preview=False, entity=entity):
                mongo_collection.update_one(
                    {'uuid': doc.get('uuid')},
                    {"$set": doc},
                    upsert=True
                )
            print(f"Collection *{entity}* has been updated!")
