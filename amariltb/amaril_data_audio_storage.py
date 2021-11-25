from google.cloud import storage
from pydub import AudioSegment
import os
from urllib.parse import urlparse
import shutil
import logging
import bz2
import pickle
import _pickle as cPickle

log = logging.getLogger(__name__)
AUDIO_CACHE_FILE = 'ad_cache.pbz2'

class AudioStorage:

    def __init__(self,credential_path):
        
        if (not credential_path):
            raise Exception("Error , use must provide credential path.")

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
        self.storage_client = storage.Client()

        self.cache = self.get_cache()


    def get_cache(self):
        cache = {}
        
        ad_cache_file_exists = os.path.isfile(AUDIO_CACHE_FILE)
        if( ad_cache_file_exists ):
            # ais:
            data = bz2.BZ2File(AUDIO_CACHE_FILE,'rb')
            cache = cPickle.load(data)

        else:
            log.info("Audio Segment Cache file does not exists")
            
        return cache


    def get_audio_segment(self,gs_full_path):

        # exists in cache ?:
        if((gs_full_path in self.cache) and  (self.cache[gs_full_path])):
            return self.cache[gs_full_path]

        # create temp directory
        result_dir='./temp'
        if(not os.path.isdir(result_dir)):
            os.mkdir(result_dir)

        # parse path:
        if(len(gs_full_path)):
            parsed_url = urlparse(gs_full_path, allow_fragments=False)
            bucket_name = parsed_url.netloc
            full_path = parsed_url.path[1:]
            filename = gs_full_path.rsplit('/', 1)[-1]
            target_path = result_dir+'/'+filename 

        else:
            return None
        try:
            # get blob:
            bucket = self.storage_client.get_bucket(bucket_name)
            blob = bucket.blob(full_path)

            # download audio:
            blob.download_to_filename(target_path) 
            audio_segment = AudioSegment.from_wav(target_path)
            
            # remove temp files:
            if(os.path.isdir(result_dir)):
                shutil.rmtree(result_dir, ignore_errors=True)

            # save to cache file:
            self.cache[gs_full_path] = audio_segment
            
            with bz2.BZ2File(AUDIO_CACHE_FILE, 'wb') as f: 
                cPickle.dump(self.cache, f)
            
            return audio_segment

        except Exception as e:
            # remove temp files:
            if(os.path.isdir(result_dir)):
                shutil.rmtree(result_dir, ignore_errors=True)
        
            log.warning('get_audio_segment failed for: '+gs_full_path+' - '+str(e))
            return None
