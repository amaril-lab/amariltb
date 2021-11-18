"""

INSTRUCTIONS:

1) install GCS: (type in VisualCode terminal:)
conda install -c conda-forge google-cloud-storage

2) install audio manipulation 3rd party lib named pydub: (type in VisualCode terminal:)
conda install -c conda-forge pydub

3) in main() change variables:
'files_per_chunk' - max audio files per unified wav/meta files. 
'prefix' - your target directory in the cloud which you want to unify:

4) the script should create a directory with 2 files types per chunk:
- a wav file named: <prefix>_<chunk_number>.wav , which is the unified wav file.
- a xlsx file named: '<prefix>_<chunk_number>_meta.xlsx', which is the meta file to be used in the unifier_meta_merge.py script

"""

# Imports:
from google.cloud import storage
import wave
from pydub import AudioSegment
import os
import shutil
from openpyxl import Workbook
import math
from datetime import datetime

class UnifierChunk:
    def __init__(self):
        self.reset()

    def reset(self):
        self.offset = 0
        self.number = 0
        self.meta_ar = []
        self.combined = AudioSegment.empty() 
        self.last_start = 0
        self.file_index = 1
        
    def process_file(self,filename,audio,processed_file_count,number,offset):
        # update chunk state:
        self.number = number
        self.offset = offset        
        
        self.meta_ar.append({
            'index':self.file_index,
            'filename':filename,
            'start':self.last_start,
            'end': self.last_start + audio.duration_seconds,
            'duration': audio.duration_seconds
        })
        self.file_index += 1
        self.last_start = self.last_start + audio.duration_seconds
        self.combined += audio

    def create_wav_file(self,target):
        target_filename_wav = target + str(self.number) + '.wav'
        self.combined.export(target_filename_wav, format="wav")

    def create_meta_file(self,target):
        target_filename_meta = target + str(self.number) + '_meta.xlsx'
        self.update_sheet(target_filename_meta,self.meta_ar)

    def is_partial(self):
        if(self.offset == 0):
            return False
        else:
            return True

    def update_sheet(self,filename,data):
        wb = Workbook()
        ws = wb.active 
        ws.title = 'result_meta'
        dest_filename = filename
        for data_item in data :
            ws.append([ data_item['filename'],data_item['start'],data_item['end'], data_item['duration'],data_item['index'] ])
        
        wb.save(filename = dest_filename)
        return

class Unifier:
    def __init__(self,credential_path,files_per_chunk):
        if not ( credential_path == None ):
            print('recieved credential_path:',credential_path)
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
        
        self.storage_client = storage.Client()
        self.files_per_chunk  = files_per_chunk

    def bubble_sort(self,arr):
        x=-1
        n=len(arr)#length of array 6
        for i in range (0,n):
            for j in range(1,n-i):
                a_j = arr[j]
                a_j_1 = arr[j-1]
                if a_j_1.timeCreated>a_j.timeCreated:
                    arr[j-1],arr[j]=arr[j],arr[j-1]
            if (n-i)<=1:
                break
        return arr
    
    def unify(self,bucket_name,prefix,temp_dir,result_dir,target):
        
        bucket = self.storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix,delimiter='/')  # Get list of files

        chunk = UnifierChunk()
        processed_file_count = 0
        errors = {}
        
        # sort blobs:
        sorted_blobs = []
        for blob in blobs:
            time_created = datetime.strptime(blob._properties['timeCreated'], '%Y-%m-%dT%H:%M:%S.%fZ')
            blob.timeCreated = time_created
            sorted_blobs.append(blob)
        
        sorted_blobs = self.bubble_sort(sorted_blobs)

        for blob in sorted_blobs:
            print('processing blob, created at:',blob.timeCreated)
            # promote file count:
            processed_file_count+=1 
            
            # current filename:
            filename = os.path.basename(blob.name)
            full_filename = temp_dir + filename 
            
            # download audio:
            try:
                blob.download_to_filename(full_filename) 
                audio = AudioSegment.from_wav(full_filename)
            except Exception as e:
                print('Error Occured - skipping over file:'+filename)
                errors[filename] = str(e)
                continue

            # update chunk state:
            chunk_number = math.floor(processed_file_count / self.files_per_chunk)
            chunk_offset = processed_file_count % self.files_per_chunk 
            chunk.process_file(filename,audio,processed_file_count,chunk_number,chunk_offset)

            # chunk full ? (write chunk and reset chunk state):
            if( not chunk.is_partial() ):
                # write files:
                target_path = os.path.join( result_dir, target)
                chunk.create_wav_file(target_path)
                chunk.create_meta_file(target_path)
                # reset chunk:
                chunk.reset()
        
        # if last chunk was partial  - write it now, since it wasnt written yet:
        if( chunk.is_partial() ):
            chunk.number += 1
            # write files:
            target_path = os.path.join( result_dir, target)
            chunk.create_wav_file(target_path)
            chunk.create_meta_file(target_path)

        print('done. errors:',errors)
        return

def unify(gcs_credential_path,bucket_name,prefix,result_dir,files_per_chunk):

    # consts:
    #bucket_name = 'recordings_test'
    #prefix = 'mturk/Animals101219/386T3MLZLO7OIABJ4XA2JTH5J9G08U___en/'
    #prefix = 'free/telaviv_adhd/hitId___heb/'
    temp_dir = 'files/'
    #result_dir = 'result/'
    target = prefix.replace('/','_') 
    #files_per_chunk = 3
    #gcs_credential_path = "able-groove-224509-b2d8d81be85b.json"

    # create directory for result:
    if(not os.path.isdir(result_dir)):
        os.mkdir(result_dir)

    # create temp directory for files downloaded:
    if(not os.path.isdir(temp_dir)):
        os.mkdir(temp_dir)

    # unify:
    unifier = Unifier(gcs_credential_path,files_per_chunk)
    unifier.unify(  bucket_name,
                    prefix,
                    temp_dir,
                    result_dir,
                    target )

    # remove temp files:
    if(os.path.isdir(temp_dir)):
        shutil.rmtree(temp_dir, ignore_errors=True)

