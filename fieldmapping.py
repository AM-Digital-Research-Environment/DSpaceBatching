# -*- coding: utf-8 -*-
"""
Created on Wed 26 June 2024

@author: AfricaMultiple (NTViswajith)
"""

# Libraries

import polars as pl
import jmespath
import itertools
from datetime import datetime
from auth_functions import *
from helper_functions import *
#from safbuilder.dspacearchive import DspaceArchive


class Mapper:

    def __init__(self, db_name, collection_name, files_folder_path=None):
        self._data = fetch_collection(db_name=db_name, collection_name=collection_name)
        self._files_folder_path = files_folder_path
        self._doc_list = []

    # Loop for row values

    def doclistbuilder(self):
        for row in self._data:
            row_dict = {
                # Filename
                'filename': row.get('bitstream'),
                # Author or Contributor
                'dc.contributor.author': try_fetch(query="name[].name[]", document=row),
                # Main title
                'dc.title': try_fetch(query="titleInfo[?title_type == 'main'].title[]",  document=row),
                # Other Titles
                'dc.title.alternative': try_fetch(query="titleInfo[?title_type != 'main'].title[]", document=row),
                # Created Dates (Start & End)
                'dc.date.createdstart': dateconvert(try_fetch(query='dateInfo.created.start',  document=row)),
                'dc.date.created': dateconvert(try_fetch(query='dateInfo.created.end',  document=row)),
                # Issued Date
                'dc.date.issue': dateconvert(try_fetch(query='dateInfo.issue.end',  document=row)),
                # Sponsor
                'dc.description.sponsorship':  try_fetch(query="sponsor[]",  document=row),
                # Citation
                'dc.description.citation': try_fetch(query="citation[]",  document=row),
                # Language
                'dc.language.iso': try_fetch(value=langmap(row.get('language')), direct=True),
                # Abstract
                'dc.description.abstract': try_fetch(query="abstract", document=row),

                # Identifiers
                # DRE Identifier
                'dc.identifier.dre': try_fetch(
                    query="dre_id", document=row
                ),
                # Local
                'dc.identifier.local': try_fetch(
                    query="identifier[?identifier_type == 'Locally defined identifier'].identifier", document=row
                ),
                # ISBN
                'dc.identifier.isbn': try_fetch(
                    query="identifier[?identifier_type == 'International standard book number'].identifier",
                    document=row
                ),
                # ISSN
                'dc.identifier.issn': try_fetch(
                    query="identifier[?identifier_type == 'International standard serial number'].identifier",
                    document=row
                ),
                # DOI
                'dc.identifier.doi': try_fetch(
                    query="identifier[?identifier_type == 'Digital object identifier'].identifier",
                    document=row
                ),
                # Other
                'dc.identifier.other': try_fetch(
                    query="""identifier[?identifier_type != 'International standard book number' &&
                    identifier_type != 'Locally defined identifier' &&
                    identifier_type != 'International standard serial number' &&
                    identifier_type != 'Digital object identifier'].identifier""",
                    document=row
                ),

                # Physical Description
                'dc.description': '\n'.join(list(itertools.filterfalse(lambda item: not item, [
                    row.get("physicalDescription").get("type"),
                    try_fetch(query="physicalDescription.method", document=row),
                    try_fetch(query="physicalDescription.desc", document=row, delimiter=", "),
                    try_fetch(query="physicalDescription.tech", document=row, delimiter=", "),
                    try_fetch(query="physicalDescription.tech", document=row, delimiter="\n")
                ]))),

                # Keywords
                'dc.subject': '||'.join(list(itertools.filterfalse(lambda item: not item, list(set(try_fetch(value=[
                    try_fetch(query="genre.*[]", document=row),
                    try_fetch(query="subject", document=row,),
                    try_fetch(query="tags", document=row,)
                ], direct=True).split("||"))))))

            }
            self._doc_list.append(row_dict)
        return self._doc_list
