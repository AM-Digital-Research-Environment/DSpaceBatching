# Dspace Batching
### Repo for DSpace Batch SAF batch creation

The batcher.py file can be used to generate the 'Simple Archive Format' file structure required for performing batch uploads to DSpace. The script will produce a file structure as the requirements stage in the DSpace's documentation with [site][1]. This includes the creation of folders dedicated to each research item with a directory name following the pattern *'item_001', 'item_002'*, and so on. Each item directory will hold at least three files, these are:  
- Bitstream file
- Contents file
- Metadata XML file *(labelled dublin_core.xml)*
- Other optional files include (currently the script does not handle these files)
  -    collections
  -    relationships

*Please make sure to use the run requirements file to make sure all dependencies are installed.* 

### Authentication for the MongoDB Client
For authentication information (MongoDB Client URI) in the auth_functions.py, please enquire us.


## To run the script please follow the below steps:  
In this step, we import the required class and instantiate said class. The following information will passed as arguments to the class,
- Your MongoDB database name
- Collection name
- File path of the folder holding all raw data. *(labelled 'files')*
~~~~

# Importing class
from batcher import batchGenerator

# Instantiating batchGenerator class
bat_gen = batchGenerator(db_name="<mongodb-database-name>", collection_name="<mongodb-collection-name>", files_folder_path="<file-path-rawdata-folder>")

~~~~

Next step (optional), you can check your metadata,  
~~~~

# To check stage metadata values
bat_gen.staged_data()

~~~~

And finally, to generate the SAF batch directory, a folder named 'batches' will be generated with items alongside the 'files' folder.
~~~~

# To generate SAF batch directory
bat_gen.create_batch_dir()

~~~~

[1]: https://wiki.lyrasis.org/pages/viewpage.action?pageId=104566653
