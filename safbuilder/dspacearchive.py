"""
This class handles the creation of a DSpace simple archive suitable for import into a dspace repository. 

See: http://www.dspace.org/1_6_2Documentation/ch08.html#N15B5D for more information about the DSpace 
Simple Archive format. 
"""

import os
import csv
from safbuilder.itemfactory import ItemFactory
from shutil import copy
import unicodedata


class DspaceArchive:

    """
    Constructor:

    The constructor takes a path to a csv file.
    It then parses the file, creates items, and adds the items to the archive.
    """
    def __init__(self,
                 file_folder_path: str | None,
                 metadata_object: csv,
                 local_object: csv,
                 datacite_object: csv,
                 dspace_object: csv,
                 relationships_object: list[str],
                 collection_name: str):
        self.items = []
        self.local_items = []
        self.datacite_items = []
        self.dspace_items = []
        self.relationships = relationships_object
        self.collection = collection_name
        if file_folder_path:
            self.input_path = file_folder_path.encode('utf-8')
            self.input_base_path = os.path.dirname(file_folder_path).encode('utf-8')


        for obj, arr in [
            [metadata_object, self.items],
            [local_object, self.local_items],
            [datacite_object, self.datacite_items],
            [dspace_object, self.dspace_items]
        ]:
            # Reading csv metadata object passed to class
            reader = csv.reader(obj.splitlines())
            header = next(reader)

            item_factory = ItemFactory(header)

            # Iterating through items
            for row in reader:
                item = item_factory.newItem(row)
                self.addItem(item, arr)

    """
    Add an item to the archive. 
    """
    def addItem(self, item, listObj: list):
        listObj.append(item)

    """
    Get an item from the archive.
    """
    def getItem(self, index):
        return self.items[index]

    """
    Write the archie to disk in the format specified by the DSpace Simple Archive format.
    See: http://www.dspace.org/1_6_2Documentation/ch08.html#N15B5D
    """
    def write(self, dir = "."):
        self.create_directory(dir)

        for index, item in enumerate(self.items):

            # item directory
            name = b"item_%03d" % (int(index) + 1)
            item_path = os.path.join(dir.encode('utf-8'), name)
            self.create_directory(item_path)

            # contents file
            self.writeContentsFile(item, item_path)

            # content files (aka bitstreams)
            self.copyFiles(item, item_path)

            # Metadata Dublin Core file
            self.writeMetadata(item, item_path)

            # Metadata Local File
            self.writeMetadata(self.local_items[int(index)],
                               schema='local',
                               file_name=b"metadata_local.xml",
                               item_path=item_path)

            # Metadata DataCite File
            self.writeMetadata(self.datacite_items[int(index)],
                               schema='datacite',
                               file_name=b"metadata_datacite.xml",
                               item_path=item_path)

            # Dspace Metadata File
            self.writeMetadata(self.dspace_items[int(index)],
                               schema='dspace',
                               file_name=b"metadata_dspace.xml",
                               item_path=item_path)

            # Collection file
            self.writeCollection(self.collection, item_path)

            # Relationship File
            self.writeRelationships(relationships_string=
                                    self.relationships[int(index)],
                                    item_path=item_path)

    """
    Create a zip file of the archive. 
    """
    def zip(self, dir=None):
        pass

    """
    Create a directory if it doesn't already exist.
    """
    def create_directory(self, path):
        if not os.path.isdir(path):
            os.mkdir(path)

    """
    Create a contents file that contains a lits of bitstreams, one per line. 
    """
    def writeContentsFile(self, item, item_path):
        contents_file = open(os.path.join(item_path, b'contents'), "wb")

        files = item.getFiles()
        for index, file_name in enumerate(files):
            contents_file.write(file_name)
            if index < len(files):
                contents_file.write(b"\n")

        contents_file.close()

    """
    Copy the files that are referenced by an item to the item directory in the DSPace simple archive. 
    """
    def copyFiles(self, item, item_path):
        files = item.getFilePaths()
        for index, file_name in enumerate(files):
            source_path = os.path.join(self.input_path,  file_name)
            dest_path = os.path.join(item_path, file_name)
            copy(source_path.decode(), self.normalizeUnicode(dest_path).decode())

    def writeMetadata(self, item, item_path, schema=None, file_name: bytes = b'dublin_core.xml'):
        xml = item.toXML(schema=schema)
        metadata_file = open(os.path.join(item_path, file_name), "wb")
        metadata_file.write(self.normalizeUnicode(xml))
        metadata_file.close()


    """
    Write relation ship file using relationship object supplied to function
    """
    def writeRelationships(self, relationships_string: str, item_path):
        try:
            relationship_file = open(os.path.join(item_path, b'relationships'), "wb")
            relationship_file.write(relationships_string.encode(encoding="utf-8"))
        except:
            print("No relationship file created.")
        finally:
            relationship_file.close()

    # Todo: Write file with collection handle
    def writeCollection(self, collection_name: str, item_path):
        try:
            collection_file = open(os.path.join(item_path, b'collections'), "wb")
            collection_file.write(collection_name.encode(encoding="utf-8"))
        except:
            print("No collection file created.")
        finally:
            collection_file.close()

    def normalizeUnicode(self, value):
        """
        Normalizes a Unicode string by replacing unicode characters with ascii equivalents.

        Args:
            str (str): The Unicode string to be normalized.

        Returns:
            str: The normalized string encoded as UTF-8.

        """
        cleaned = unicodedata.normalize(u'NFD', value.decode()).encode('ascii', 'ignore')
        return cleaned.decode().encode('utf-8')
