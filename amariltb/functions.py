from datetime import datetime
import os
import codecs
import json
import pathlib
from .pw_converter_db import DynamoDB
from .pw_converter_utils import update_sheet
from . import pw_converter as converter
from . import audio_unifier_meta_merger as merger
from . import audio_unifier as wav_unifier

import boto3


def init_DB(version,config_path = None):
    """Amaril CLI offers the following commands: (for help type: amaril_cli COMMAND --help)"""
    
    # CONFIG:
    # construct default config path:
    if (not config_path):
        current_path = pathlib.Path().absolute()
        config_path = current_path
    config_dir =  os.path.join(config_path, 'config')
    gcs_credential_filename =  os.path.join(config_dir, 'able-groove-224509-b2d8d81be85b.json')
    aws_credential_filename =  os.path.join(config_dir, 'aws_cred.json')

    # if doesnt exist - prompt user:

    #print('recieved GCS credential_path:',gcs_credential_filename)
    #print('recieved AWS credential_path:',aws_credential_filename)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcs_credential_filename
    
    # load AWS json: 
    aws_credentials = json.load(codecs.open(aws_credential_filename, 'r', 'utf-8-sig'))
    # TBD: set env variables        
    os.environ['AWS_ACCESS_KEY_ID'] = aws_credentials["aws_access_key_id"]
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_credentials["aws_secret_access_key"]
    os.environ['REGION_NAME'] = aws_credentials["region_name"]
    
    # init DB:
    if(not DynamoDB.instance):
        print('init_DB')
        DynamoDB.instance = boto3.resource(  'dynamodb',
                            aws_access_key_id= os.environ['AWS_ACCESS_KEY_ID'],
                            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                            region_name=os.environ['REGION_NAME'] )
    DynamoDB.version = version

"""unifies wave files from a directory in the cloud, to chunks localy."""
def unify_waves(bucket_name, source_dir,target_dir,files_per_chunk):
    # TBD: separate google config out of init_DB:
    init_DB("TEST")
    wav_unifier.unify(None,bucket_name,source_dir,target_dir,files_per_chunk)

def get_index(language,category,target_filename,version):    
    init_DB(version)
    converter.handle_get_index(language,category,target_filename)

"""merges sonix product and meta files, and processes into the ProductWords data base table."""
def process_sonix(input_dir,target_filename, language,category,version):
    init_DB(version)
    data = merger.merge_meta_into_sonix(input_dir,target_filename)
    converter.convert_sonix_data(data,language,category)

"""processes a PRRAT grid (excel file) into the ProductWords data base table."""
def process_praat(input_filename, language,category,version):
    init_DB(version)
    converter.convert_prrat(input_filename,language,category)

"""make the following changes:
- remove the index from the index item,
- add the transform 
""" 
def add_transform(word,transform,category,language,version):
    init_DB(version)
    converter.handleAddTransform(word,transform,language,category)

"""make the following changes:
- remove the transform from the index item,
- add a new index item for the raw word.
- apply this change to products (existing product words change: transform-->rawWord, and  index change accordingly
"""
def remove_transform(word,language,category,version):
    init_DB(version)
    converter.handleRemoveTransform(word,language,category)

"""updates index table in data base, from  the given tranforms in excel source file."""
def add_transforms(source_filename,version):
    init_DB(version)
    converter.handleAddTransforms(source_filename)

"""delete all product words asociated with filename (doesnt delete from index)"""
def delete_product_words(filename,version):
    init_DB(version)
    converter.handleDeleteProductWords(filename,DynamoDB().get_table("productWords"))

"""export to excell, all product words asociated with filename """
def get_product_words(filename,target_filename,version):
    init_DB(version)
    items = converter.handleGetProductWords(filename)
    update_sheet(target_filename,items)

"""export to excell, all product words asociated with filename """
def get_product_words_by_category(category,language,target_filename,version):
    init_DB(version)
    items = converter.handleGetProductWordsByCategory(category,language)
    update_sheet(target_filename,items)
    
""" deletes word from index and all occurrences of the word in productWords."""
def delete_from_index(word,language,category,version):
    print('delete_from_index')
    init_DB(version)
    converter.handle_delete_from_index(word,language,category)

"""processes a directory of PRRAT grids (excel file) into the ProductWords data base table."""
def process_praat_dir(input_dir, language,category,version):
    init_DB(version)
    converter.process_praat_dir(input_dir,language,category)


def listChunker(lst, csize:int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), csize):
        yield lst[i:i + csize]

def report(msg:str):
    _time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print(f"{_time}: {msg}")

def weirdCase(targetString:str):
    returnWord:str = ""
    for i,letter in enumerate(targetString):
        i += 1
        if (i % 2 == 0):
            returnWord += letter.lower()
        else:
            returnWord += letter.upper()
    return returnWord