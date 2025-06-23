# DSpace Batching (EN)
### Repository for DSpace SAF Batch Creation

The `batcher.py` file can be used to generate the *Simple Archive Format* (SAF) file structure required for performing batch uploads to DSpace. The script produces a file structure as specified in the DSpace documentation ([see here](https://wiki.lyrasis.org/display/DSDOC9x/Importing+and+Exporting+Items+via+Simple+Archive+Format)). This includes the creation of folders for each research item, with directory names following the pattern *'item_001', 'item_002'*, and so on. Each item directory will contain at least three files:

- Bitstream file  
- Contents file  
- Metadata XML file *(named `dublin_core.xml`)*  
- Other optional files (currently not handled by the script):  
  - `collections`  
  - `relationships`  

**Please ensure you use the `requirements.txt` file to install all necessary dependencies.**

### Authentication for the MongoDB Client

For authentication (MongoDB Client bot URI) in `auth_functions.py`, please fill in the `auth_functions_config.json` file or contact us for assistance.

---

## To run the script, follow these steps:

First, import the required class and instantiate it. Provide the following arguments:

- Your MongoDB database name  
- Collection name  
- File path to the folder containing all raw data *(named `files`)*

```python
# Importing the class
from batcher import batchGenerator

# Instantiating the batchGenerator class
bat_gen = batchGenerator(
    db_name="<mongodb-database-name>",
    collection_name="<mongodb-collection-name>",
    files_folder_path="<file-path-to-rawdata-folder>"
)

# To check staged metadata values (Optional)
bat_gen.staged_data()

# To generate the SAF batch directory
bat_gen.create_batch_dir()
```
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Traitement par lots DSpace (FR)
### Dépôt pour la création de lots SAF pour DSpace

Le fichier `batcher.py` peut être utilisé pour générer la structure de fichiers *Simple Archive Format* (SAF) requise pour effectuer des téléversements par lots dans DSpace.  
Le script produit une structure de fichiers conforme à la documentation de DSpace ([voir ici](https://wiki.lyrasis.org/display/DSDOC9x/Importing+and+Exporting+Items+via+Simple+Archive+Format)).

Cela inclut la création de dossiers pour chaque élément de recherche, avec des noms de répertoire suivant le modèle *'item_001', 'item_002'*, etc.  
Chaque dossier d’élément contiendra au moins trois fichiers :

- Fichier Bitstream  
- Fichier `contents`  
- Fichier XML de métadonnées *(nommé `dublin_core.xml`)*  
- Autres fichiers optionnels (actuellement non pris en charge par le script) :  
  - `collections`  
  - `relationships`  

**Veuillez vous assurer d’utiliser le fichier `requirements.txt` pour installer toutes les dépendances nécessaires.**

---

### Authentification pour le client MongoDB

Pour l’authentification (URI du bot client MongoDB) dans `auth_functions.py`, veuillez remplir le fichier `auth_functions_config.json` ou nous contacter pour obtenir de l’aide.

---

## Pour exécuter le script, suivez ces étapes :

Commencez par importer la classe requise et l’instancier. Fournissez les arguments suivants :

- Nom de votre base de données MongoDB  
- Nom de la collection  
- Chemin du dossier contenant toutes les données brutes *(nommé `files`)*

```python
# Importation de la classe
from batcher import batchGenerator

# Instanciation de la classe batchGenerator
bat_gen = batchGenerator(
    db_name="<nom-base-de-données-mongodb>",
    collection_name="<nom-collection-mongodb>",
    files_folder_path="<chemin-vers-dossier-données-brutes>"
)

# Pour vérifier les valeurs des métadonnées en attente
bat_gen.staged_data()

# Pour générer le répertoire de lot SAF
bat_gen.create_batch_dir()
```
