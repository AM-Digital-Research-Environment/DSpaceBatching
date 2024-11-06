# RDSpace@UBT Batching 
### Repository for SAF batches generation for EXC-Africa Multiple Research Data 

The batcher.py file can be used to generate the 'Simple Archive Format' directory required for performing batch uploads to DSpace. The script will produce a file structure as per the requirements stated in DSpace's [documentation site][1]. This includes the creation of folders dedicated to each research item with a directory name following the pattern *'item_001', 'item_002'*, and so on. Each item directory will hold the following files:  
- Bitstream file (or data file)
- Contents file
- Metadata XML files
  - General metadata *(labelled dublin_core)*
  - Datacite specific metadata *(labelled metadata_datacite)*
  - DSpace specific metadata *(labelled metadata_dspace)*
  - And, System specific local metadata *(labelled metadata_local)*
- Other optional files include (currently the script does not handle these files)
  -    collections
  -    relationships

NOTICE  
*Please make sure to run requirements file in the repository to ensure all dependencies are installed. Furthermore, please ensure that the 'dicts/auth.json' is populated with required credentials before executing the script* 

#### Things to bear in mind before you begin and perform the upload:
- Firstly, ensure that projects have data files for upload.
- Secondly, ensure that,
  - access condition values set for each data item in MongoDB.
  - the appropriate license is the has been set for the data item.
  - the bitstream name on MongoDB matches the name of the data item in files directory.
- Finally, when uploading zipped file run 'Validate Only' process to check for the potentials metadata linkage error on RDSpace.  

## To run the script please follow the below steps:  
In this step, we import the required class and instantiate said class. The following information will passed as arguments to the class,
- Your MongoDB database name
- Collection name
- File path of the folder holding all raw data. *(labelled 'files')*
~~~~

# Importing class
from batcher import BatchGenerator

# Instantiating batchGenerator class
bat_gen = BatchGenerator(db_name="<mongodb-database-name>", collection_name="<mongodb-collection-name>", files_folder_path="<file-path-rawdata-folder>")

~~~~

Next step (optional), you can check your metadata,  
~~~~

# To check stage metadata values
staged = bat_gen.create_batch_dir(stage=True)

~~~~

And finally, to generate the SAF batch directory, a folder named 'batches' will be generated with items alongside the 'files' folder.
~~~~

# To generate SAF batch directory
bat_gen.create_batch_dir()

~~~~

[1]: https://wiki.lyrasis.org/pages/viewpage.action?pageId=104566653
