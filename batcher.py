# -*- coding: utf-8 -*-
"""
Created on Wed 26 June 2024

@author: AfricaMultiple (NTViswajith)
"""

# Libraries

import polars as pl
import os
import itertools
import jmespath as jp
from auxiliary.auth_functions import *
from auxiliary.helper_functions import *
from safbuilder.dspacearchive import DspaceArchive


class BatchGenerator:

    def __init__(self, db_name, collection_name,
                 files_folder_path=None,
                 main_project: str = "c382517d-8e02-4932-859b-35b195219119"):
        self._data = fetch_collection(db_name=db_name, collection_name=collection_name)
        self._project = fetch_collection(db_name='dev', collection_name='projectInfo',
                                         is_dev=True, query={"Project_ID": collection_name})[0]
        self._main_project = main_project
        self._license_data = jp.search(
            '[].{uuid:uuid, name:name, identifier:metadata."dc.identifier.spdx-id"[].value | [0]}',
            fetch_collection(
                db_name="dspace_metadata_ubt", collection_name="licenses", is_dev=True
            )
        )
        self._files_folder_path = files_folder_path
        self._relationship_types = json_file("dicts/relationsSchema.json")

    # Loop for row values

    def doclistbuilder(self):
        _doc_list = []
        for row in self._data:
            row_dict = {
                # Filename
                'filename': row.get('bitstream'),
                # Main Title
                schemamap('title'): try_fetch(query="titleInfo[?title_type == 'main'].title[]",  document=row),
                # Data of Issue (format "yyyy-mm-dd")
                schemamap('dateIssue'): datetime.today().strftime("%Y-%m-%d"),
                # Date of Creation (end)
                schemamap('dateCreated'): dateconvert(try_fetch("dateInfo.created.end", document=row)),
                # Resource Type
                schemamap('generalResourceType'): typemap(row.get('typeOfResource')),
                # Langauge
                schemamap('language'): try_fetch(value=langmap(row.get('language')), direct=True),
                # Subject Keywords
                schemamap('subjectKeywords'): '||'.join(list(itertools.filterfalse(
                    lambda item: not item, list(set(try_fetch(value=[
                        try_fetch(query="genre.*[]", document=row),
                        try_fetch(query="subject[].origLabel", document=row,),
                        try_fetch(query="tags", document=row,)
                    ], direct=True).split("||")))))),
                # Abstract
                schemamap('abstract'): try_fetch(query="abstract", document=row),
                # Technical Information
                schemamap('technicalInformation'): '||'.join(list(itertools.filterfalse(lambda item: not item, [
                    row.get("physicalDescription").get("type"),
                    try_fetch(query="physicalDescription.method", document=row),
                    try_fetch(query="physicalDescription.desc", document=row, delimiter=", "),
                    try_fetch(query="physicalDescription.tech", document=row, delimiter=", "),
                ]))),
            }
            # Optional Fields
            """
            Date of collection not included in this list
            """

            # Date Fields
            # Date of Creation (start)
            checkAppend(
                row_dict,
                schemamap('dateCreatedStart'),
                try_fetch(query="dateInfo.created.start", document=row), isdate=True)
            # Date of Validity (start & end)
            # Start
            checkAppend(
                row_dict,
                schemamap('dateValidStart'),
                try_fetch(query="dateInfo.valid.start", document=row), isdate=True)
            # End
            checkAppend(
                row_dict,
                schemamap('dateValidEnd'),
                try_fetch(query="dateInfo.valid.end", document=row), isdate=True)
            # Date of Copyright (only end)
            checkAppend(
                row_dict,
                schemamap('dateCopyright'),
                try_fetch(query="dateInfo.copy.end", document=row), isdate=True)

            # Title Types
            # Subtitle
            checkAppend(
                row_dict,
                schemamap('subtitle'),
                try_fetch(query="titleInfo[?title_type == 'Sub'].title[]", document=row))
            # Translated
            checkAppend(
                row_dict,
                schemamap('transTitle'),
                try_fetch(query="titleInfo[?title_type == 'Translated'].title[]", document=row))
            # Alternative
            checkAppend(
                row_dict,
                schemamap('altTitle'),
                try_fetch(query="titleInfo[?title_type == 'Alternative'].title[]", document=row))

            # Contributor List
            """
            In the case of the contributor role assigned contributor name must be added as qualifier,hence this 
            requires in the creation of the custom fields to match the qualifier. All elements will fall under the
            schema dc.contributor 
            """
            # Fetching unique qualifiers
            for role_type in list(set(jp.search("name[].role", row))):
                row_dict["dc.contributor.{role_type]"] = try_fetch(
                    query=f"name[?role == {role_type}].name.label",
                    document=row
                )

            _doc_list.append(row_dict)
        return _doc_list

    # ToDo: Relationships Dictionary
    def relationshipsbuilder(self):
        _relations_doc_list = []
        for row in self._data:
            _relations_list = [
                " ".join([self._relationship_types.get('projectMain'), self._main_project])
            ]
            # License
            if row.get('accessCondition').get('rights'):
                for l in row.get('accessCondition').get('rights'):
                    _relations_list.append(
                        " ".join(
                            [
                                self._relationship_types.get('license'),
                                jp.search(f"[?identifier == '{l}'].uuid | [0]", self._license_data)
                            ]
                        ))

            # Sub-Project (i.e., project name)
            _relations_list.append(
                " ".join(
                    [
                        self._relationship_types.get('projectSub'),
                        self._project.get('rdspace').get('projectSub').get('uuid')
                    ]
                )
            )
            _relations_doc_list.append("\n".join(_relations_list))
        return _relations_doc_list

    # Staged values or pre-view object
    def staged_data(self):
        return pl.DataFrame(self.doclistbuilder()).write_csv(file=None)

    # Staged Related Entities
    def staged_relations(self):
        return self.relationshipsbuilder()

    # Create batches
    def create_batch_dir(self):
        archive = DspaceArchive(
            file_folder_path=self._files_folder_path,
            metadata_object=self.staged_data(),
            relationships_object=self.relationshipsbuilder(),
            collection_name=self._project.get('rdspace').get('collection').get('uuid')
        )
        archive.write(os.path.dirname(self._files_folder_path) + "\\batches")
