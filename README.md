# Amaril Toolbox
tool box for amaril lab.

## Installation and updating
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install Amaril Toolbox like below. 
Rerun this command to check for and install  updates with the flag: --upgrade.
```bash
pip install git+https://github.com/amaril-lab/amariltb
```

## Usage
possible values for version parameter:
* "TEST" -  tables used are: "IndexTest", "ProductWordsTest"
* "V1" - tables used are: "Index", "ProductWords"
* "V2" - tables used are: "IndexV2", "ProductWordsV2"

possible values for language parameter:
* "hebrew" 
* "english"

#### Examples of usage of some of the features:
```python
import amariltb

# GET INDEX:
language = 'english'
category = 'animals'
target_filename = 'index_testing.xlsx'
version = 'V1'
amariltb.functions.get_index(language,category,target_filename,version)


# UNIFY WAVES:
bucket_name = "recordings_test"
source_dir = "free/mbh040821/hitId___heb/" 
target_dir = "/Users/shaharhalutz/Documents/Projects/Amaril/amaril-toolkit-module-poc/" 
files_per_chunk = 15
amariltb.functions.unify_waves(bucket_name, source_dir,target_dir,files_per_chunk)

# PROCESS SONIX:
version = "TEST"
language = "english"
category = "animals" 
input_dir = "/yourPathToTheRelevantDirectory/" 
amariltb.functions.process_sonix(input_dir,target_filename, language,category,version)

# PROCESS PRRAT:
version = "TEST"
input_filename = "/yourPathToTheRelevantDirectory/testing_prrat.xlsx"
language = "hebrew"
category = "animals" 
amariltb.functions.process_praat(input_filename, language,category,version)

# ADD TRANSFORM:
version = "TEST"
language = "hebrew"
category = "animals" 
word = "סיח"
transform = "סוס"
amariltb.functions.add_transform(word,transform,category,language,version)

# REMOVE TRANSFORM:
version = "TEST"
language = "hebrew"
category = "animals" 
word = "סיח"
amariltb.functions.remove_transform(word,language,category,version)

# DELETE FROM INDEX:
version = "TEST"
language = "hebrew"
category = "animals" 
word = "סוס"
amariltb.functions.delete_from_index(word,language,category,version)

```

## Conda environments:
if you dont want to polute your main python environment with the amariltb package dependencies , 
you can opt to use conda envirounments. 
a  cheat sheet for handling envirounments: 
https://docs.conda.io/projects/conda/en/4.6.0/_downloads/52a95608c49671267e40c689e0bc00ca/conda-cheatsheet.pdf