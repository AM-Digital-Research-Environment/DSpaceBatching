# -*- coding: utf-8 -*-
"""
Created on Wed 26 June 2024

@author: AfricaMultiple (NTViswajith)
"""

# Libraries

import polars as pl
import os
import itertools
from auxiliary.auth_functions import *
from auxiliary.helper_functions import *
from safbuilder.dspacearchive import DspaceArchive


class batchGenerator:

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
                # Main Title
                schemamap('title'): try_fetch(query="titleInfo[?title_type == 'main'].title[]",  document=row),
                # Creator
                # TODO: Creator Section (this section must exclude named types)
                # Data of Issue
                # TODO: Issue Date (Check what value needs to be passed here)
                # Resource Type
                # TODO: General resource type and type dictionary to be setup
                schemamap('resourceType'): row.get('typeOfResource'),
                #schemamap('generalResourceType'): typemap(row.get('typeOfResource'))
                # Langauge
                schemamap('language'): try_fetch(value=langmap(row.get('language')), direct=True),
                # Subject Keywords
                schemamap('subjectKeywords'): '||'.join(list(itertools.filterfalse(
                    lambda item: not item, list(set(try_fetch(value=[
                    try_fetch(query="genre.*[]", document=row),
                    try_fetch(query="subject", document=row,),
                    try_fetch(query="tags", document=row,)
                    ], direct=True).split("||")))
                ))),
                # Abstract
                schemamap('abstract'): try_fetch(query="abstract", document=row),
                # Technical Information
                schemamap('technicalInformation'): '||'.join(list(itertools.filterfalse(lambda item: not item, [
                    row.get("physicalDescription").get("type"),
                    try_fetch(query="physicalDescription.method", document=row),
                    try_fetch(query="physicalDescription.desc", document=row, delimiter=", "),
                    try_fetch(query="physicalDescription.tech", document=row, delimiter=", "),
                ]))),
                # Creation Dates
            }
            self._doc_list.append(row_dict)
        return self._doc_list

    #TODO: Relationships Dictionary
    def relationshipsBuilder(self):
        # Fields to be added in the relationships dictionary
        ## Licence
        ## Contributors?
        ## Main Project and Funding
        ## Subprojects
        ## Organisational Assignment
        pass

    # Staged values or pre-view object
    def staged_data(self):
        return pl.DataFrame(self.doclistbuilder()).write_csv(file=None)

    # Create batches
    def create_batch_dir(self):
        archive = DspaceArchive(self._files_folder_path, self.staged_data())
        archive.write(os.path.dirname(self._files_folder_path) + "\\batches")

