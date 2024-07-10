# -*- coding: utf-8 -*-
"""
Created on Wed 26 June 2024

@author: AfricaMultiple (NTViswajith)
"""
# Libraries
import pandas as pd
import json
from datetime import datetime
import jmespath


# Function for the json data retrieval


def json_file(file_path: str):
    with open(file_path) as file_obj:
        try:
            return json.load(file_obj)
        finally:
            file_obj.close()


# Try fetch


def try_fetch(query=None, document=None, value=None, delimiter="||", direct=False):
    try:
        if direct:
            return_value = value
        else:
            return_value = jmespath.search(query, document)
        if isinstance(return_value, list):
            if len(return_value) > 1:
                return delimiter.join([x for x in return_value if x is not None])
            elif len(return_value) == 1 and not pd.isna(return_value[0]):
                return return_value[0]
            else:
                pass
        elif not pd.isna(return_value):
            return return_value
        else:
            pass
    except KeyError:
        pass

# Date Convert


def dateconvert(value):
    if value is not None:
        return datetime.fromisoformat(value).strftime("%d-%m-%Y")
    else:
        return None


# Language Mapping


def langmap(value_list):
    lang_list = json_file("dicts/lang.json")
    if len(value_list) != 0:
        return [try_func(l, lambda x: lang_list.get(x)) for l in value_list]
    else:
        pass

# Schema name/label mapping


def schemamap(label):
    schema_list = json_file("dicts/schema.json")
    return schema_list.get(label)


# Try Function (NameError)


def try_func(value, func):
    try:
        if func(value) is None:
            return value
        else:
            return func(value)
    except NameError:
        return value
