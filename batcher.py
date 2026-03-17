# -*- coding: utf-8 -*-
"""
Created on Wed 26 June 2024

@author: AfricaMultiple (NTViswajith)
"""

# Libraries

import polars as pl
import os
import glob
import jmespath as jp
import itertools
from auxiliary.auth_functions import *
from auxiliary.helper_functions import *
from safbuilder.dspacearchive import DspaceArchive


class BatchGenerator:

    def __init__(self, db_name, collection_name, files_folder_path=None):
        self._data = fetch_collection(db_name=db_name, collection_name=collection_name)
        self._files_folder_path = files_folder_path


    # Loop for row values

    def doclistbuilder(self):
        _doc_list = []
        for row in self._data:
            row_dict = {
                # Filename
                'filename': row.get('bitstream'),
                # Author or Contributor
                'dc.contributor.author': try_fetch(query="name[?name.qualifier == 'person'].name.label", document=row),
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
                'dc.description': '||'.join(list(itertools.filterfalse(lambda item: not item, [
                    row.get("physicalDescription").get("type"),
                    try_fetch(query="physicalDescription.method", document=row),
                    try_fetch(query="physicalDescription.desc", document=row, delimiter=", "),
                    try_fetch(query="physicalDescription.tech", document=row, delimiter=", "),
                ]))),

                # Keywords
                'dc.subject': '||'.join(list(itertools.filterfalse(lambda item: not item, list(set(try_fetch(value=[
                    try_fetch(query="genre.*[]", document=row),
                    try_fetch(query="subject[].origLabel", document=row,),
                    try_fetch(query="tags", document=row,)
                ], direct=True).split("||"))))))

            }
            _doc_list.append(row_dict)
        return _doc_list

    def check_files_directory(self) -> dict[str, str | list[str]]:

        # List of bitstream names in MongoDB
        mongo_list = set(jp.search('[?bitstream != \'\'].bitstream', self._data))

        # List of filenames in directory
        files_in_folder = set([os.path.basename(f) for f in glob.glob(self._files_folder_path + "/*")])

        missing_in_mongo = list(files_in_folder - mongo_list)
        missing_in_files_folder = list(mongo_list - files_in_folder)

        if not missing_in_mongo and not missing_in_files_folder:
            return {"message": "Both lists contain the same filenames."}

        return {
            "message": "Please make sure the file names on MongoDB and your files directory match.",
            "mongo_missing_list": missing_in_mongo,
            "folder_missing_list": missing_in_files_folder
        }

    # Staged values or pre-view object
    def staged_data(self):
        return pl.DataFrame(self.doclistbuilder())

    # Create batches
    def create_batch_dir(self):
        archive = DspaceArchive(self._files_folder_path, self.staged_data().write_csv(file=None))
        archive.write(os.path.dirname(self._files_folder_path) + "\\batches")
