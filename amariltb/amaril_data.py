# TBD: debug printout : who are the filenames that participate in the no column part of the data
# TBD: make sure no Fix is added to these ... (and if not remove them from list free_ADHDfacebook_230620_hitId___heb_1.xlsx)

import os
import json
import codecs

import boto3
import pathlib
from google.cloud import storage
from google.oauth2 import service_account

from boto3.dynamodb.conditions import Key, Attr,Not
import pickle
from .amaril_data_normelizer import NormelizeDiagnoses,NormelizeMeds,E_Diagnosis
from .amaril_data_filters import C_Attributes
import logging
import numpy as np
from .amaril_data_participant import Participant
from .amaril_data_audio_storage import AudioStorage 

 
C_DEBUG_FILENAME = 'debug_log.txt'
logging.basicConfig(filename=C_DEBUG_FILENAME,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.WARNING)
log = logging.getLogger(__name__)

C_Config_Filename = 'aws_cred.json'
C_Config_Dirname =  'config'
C_AI_Table =  "Assignments"
C_PW_Table =  "ProductWords"




class AmarilData:

    def __init__(self,filters,use_local_db = True,use_snapshot=False,ad_cache_len=50,local_ai_db_path='ais_data.pickle',local_pw_db_path='pws_data.pickle',local_index_db_path='index_data.pickle',cred_filename=C_Config_Filename):

        self.log_warnings = 0
        self.log_errors = 0

        if(use_snapshot):
            self.load_snapshot(use_local_db,local_ai_db_path,local_pw_db_path,local_index_db_path,cred_filename)
        else:
            self.load_data(use_local_db,local_ai_db_path,local_pw_db_path,local_index_db_path,cred_filename)
        
        self.init_db_instance()
        self.data = self.filter_v2(filters)
        
        # generate data:
        self.participants = self.create_participants_v2(self.data)
        self.data_frame_dict = self.generate_data_frame(self.participants)

        gcs_credential_path = "./config/able-groove-224509-b2d8d81be85b.json"
        self.audio_storage = AudioStorage(gcs_credential_path,ad_cache_len)
        
        self.print_result()


    def hidrate_participants_audio_segments(self):

        for participant in self.participants:

            gs_full_path = participant.get_audio_gs_full_path()
            
            if(gs_full_path):
                participant_audio_segment = self.audio_storage.get_audio_segment(gs_full_path)
                participant.set_audio_segment(participant_audio_segment)
                    
            else:
                log.warning('hidrate_participants_audio_segments at id:'+participant.data_item['id']+' gs_full_path missing.')

                
            

    def print_result(self):
        print('result:')
        print('filtered participants: '+str(len(self.participants)))
        print('errors:',self.log_errors)
        print('warnings:',self.log_warnings)



    
    def get_index(self,category,language):

        output_index = []
        for index_item in self.indexes[category+"_"+language]:
            if("index" in index_item):
                output_index.append([int(index_item["index"]),index_item["word"]]) 
        
        sorted_output_index = sorted(output_index, key=lambda k: k[0])
        del sorted_output_index[0]
        
        return(sorted_output_index)
    
    def load_local_data(self,local_ai_db_path,local_pw_db_path,local_index_db_path):
        # ais:
        with open(local_ai_db_path,'rb') as f:
            self.all_ais = pickle.load(f)
        
        # pws:
        with open(local_pw_db_path,'rb') as f:
            self.all_ais_pws_dict = pickle.load(f)
        
        # index:
        with open(local_index_db_path,'rb') as f:
            self.indexes = pickle.load(f)
    

    def save_local_data(self,local_ai_db_path,local_pw_db_path,local_index_db_path):
        
        with open(local_pw_db_path, 'wb') as f:
            pickle.dump(self.all_ais_pws_dict, f)

        with open(local_ai_db_path, 'wb') as f:
            pickle.dump(self.all_ais, f)  

        with open(local_index_db_path, 'wb') as f:
            pickle.dump(self.indexes, f)       

    def init_db_instance(self):
        # load AWS Config file:
        current_path = pathlib.Path().absolute()
        config_dir =  os.path.join(current_path, C_Config_Dirname)
        aws_credential_filename =  os.path.join(config_dir, C_Config_Filename)
        aws_credentials = json.load(codecs.open(aws_credential_filename, 'r', 'utf-8-sig'))

        # init DB:
        self.db_instance = boto3.resource(  'dynamodb',
                            aws_access_key_id= aws_credentials["aws_access_key_id"],
                            aws_secret_access_key=aws_credentials["aws_secret_access_key"],
                            region_name= aws_credentials["region_name"] )
    
    def load_db_data(self,cred_filename):
        
        self.init_db_instance()

        self.indexes = self.db_get_indexes()
        self.all_ais = self.db_get_ais()
        self.all_ais_pws_dict = self.db_get_pws()
        #self.test_ts()
        self.repair_missing_ai_ids()
        self.remove_pws_start_greater_than(65)

        return

    def test_ts(self):
        ai_id_list = [
"2MINPG1Q7GX4QPQMCK5NEM4VTWXIS0",
"CHB5TGL55FXQM5NX8KRIB71T12V6E3",
"JCVH8EKRO0KOX12N8XIM2I3IF0LJ3B",
"S7OCUAOA5DMRIWTRKKO73YWRS6EOP5",
"JP0HR7NEUFRTYLQC1HPR19K0J88M3A",
"S639X0U2RWP7J2NE6FMM2ABXVECX91",
"2ZQA6S78DCHDGIVUVY5FJHPIOSPWG9",
"GDJHS3G01OSBSC2UNB86P7ZSDP2BJ3",
"IKFG1V3U5XMKHJBE84EHO4FAGHV968",
"A385SHAH0E1T41L2PF4F9W5GQYC7MT",
"47H9IOYAFIZ4FQ34GJS5FXX8OXJPM0",
"4GEWZ9JR0YR4J50DGOVZM3225UP5OF",
"7FL7KWABQ7I6NL9O4EDI6KNN4XC5ZB",
"YM2V7ILCH2REA9O5C9FE1T16AFCXM7",
"SL22D5WTMNK1ECG1OKO7RFEOSHUJM4",
"R5L3NL6DVSX0B47SLFEETGMEOWOINY",
"5NIIJ8B9QPFH6HMI3LXDBYX6IW0YQQ",
"X5T261X7P3MI1BN6155R2WDL5JNCAT",
"XHLC64REP1N8XLJCVZUR55Q3MU91N9",
"L67CO485EYEEVG5UMFMGFG7WOIYMOM",
"5NX5ABW62RIJFFM2N0X25V82W8J1NE",
"FX0PPONQPF469S5B19L7BW67K093BM",
"CHIZMIPECW5B8J5TWV0RM4UUBYSQOR",
"M93KLNVV4PA43PYTPO3IOYSAZD4HOF",
"VS3HKCXYN79BBP7ILUPD36NZZGH5X1",
"03REH38FEPK2LFD7M9OVE32OUA5JA0",
"GTWIUU9S34IUN8YHUAHLQ7XT8M3ZXF",
"8UBLNPDBQ2ZD6BKDBUONT34T34SGAG",
"EYU2M8E8LR04FJFLKA00JRIOSV0UYI",
"U84EBJP2J1UEITBSN9Z44WK887HNAA",
"21ec7d0c16d0f3c52125fbb41a85ca4baf0",
"2586af1434d3239c8d5e8d8374c837dade0",
"62dcffd3f9554e4ba8a0ff21304d0d15522",
"e90ebc9b3d2555467a45f5fe381cbaa116b",
"98e048ce4b31a7317d9a2c378448cc9c2c9",
"26414c60ca9a421f10d57dda8a765a27fc2",
"f1fef212aff0f931a234837652ae7f6c11f",
"5707cec9842ef281d36165159437294c594",
"1b887357cb9d9868f157b584f73b9c22b24"
        ]

        for ai in self.all_ais:
            if(ai['id'] in ai_id_list ):
                print('ai id:'+ai['id'] +' dig: ' + str(ai['diagnoses']))

    
    def load_snapshot(self,use_local_db,local_ai_db_path,local_pw_db_path,local_index_db_path,cred_filename):

        storage_client = None
        if( ('GOOGLE_APPLICATION_CREDENTIALS_JSON_DATA' in os.environ) and  
            os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON_DATA']):
            
            creds_json =  os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON_DATA']
            gcp_json_credentials_dict = json.loads(creds_json)
            credentials = service_account.Credentials.from_service_account_info(gcp_json_credentials_dict)
            storage_client = storage.Client(project=gcp_json_credentials_dict['project_id'], credentials=credentials)
        else:
            # try local file
            gcs_credential_path = "./config/able-groove-224509-b2d8d81be85b.json"
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcs_credential_path
            storage_client = storage.Client()
      
        snap_id = 'snap_8_2_20/'

        # ais:
        bucket = storage_client.bucket('amaril_data_snapshots')
        blob = bucket.blob(snap_id+local_ai_db_path)
        pickle_in = blob.download_as_string()
        self.all_ais =  pickle.loads(pickle_in)

        
        # pws:
        blob = bucket.blob(snap_id+local_pw_db_path)
        pickle_in = blob.download_as_string()
        self.all_ais_pws_dict =  pickle.loads(pickle_in)

        
        # index:
        blob = bucket.blob(snap_id+local_index_db_path)
        pickle_in = blob.download_as_string()
        self.indexes =  pickle.loads(pickle_in)

    
    def load_data(self,use_local_db,local_ai_db_path,local_pw_db_path,local_index_db_path,cred_filename):

        ais_file_exists = os.path.isfile(local_ai_db_path)
        pws_file_exists = os.path.isfile(local_pw_db_path)
        index_file_exists = os.path.isfile(local_index_db_path)

        # use_local_db ?
        if( use_local_db and
            ais_file_exists and
            pws_file_exists and
            index_file_exists ):

            self.load_local_data(local_ai_db_path,local_pw_db_path,local_index_db_path)

        else:

            self.load_db_data(cred_filename)
            self.save_local_data(local_ai_db_path,local_pw_db_path,local_index_db_path)      

    def filter_pws(self,filters,ais_dict):

        pw_ar_items =  list(ais_dict.values())
        
        ai_ids_by_pw_filter = []
        for pw_item in pw_ar_items:

            item_to_filter = self.get_first_none_start_pw(pw_item)
            if(item_to_filter):
                
                passed = True
                for filter in filters:
                    if(not filter.passed(item_to_filter,self.all_ais_pws_dict)):
                        passed = False
                        break
                
                if(passed):
                    ai_ids_by_pw_filter.append(item_to_filter['assignmentId'])

        filtered_pws = {k: ais_dict.get(k, None) for k in ai_ids_by_pw_filter}
        return filtered_pws

    def get_first_none_start_pw(self,pw_ar):
        if(pw_ar and len(pw_ar) > 0):
            for pw in pw_ar:
                if( pw and
                    ('word' in pw ) ):
                    if (pw['word'] == 'start' ):
                        continue
                    else:
                        return(pw) 
        return None

    def remove_pws_start_greater_than(self,secs_thresh):

        ai_ids_to_remove = []
        filenames = []
        for pw_ar in list(self.all_ais_pws_dict.values()):
            last_pw_item =  pw_ar[-1]
            # so skip this ai ?
            if(last_pw_item == None):
                continue

            if last_pw_item['start'] > secs_thresh:
                log.warning('going to remove pw with start:'+str(last_pw_item['start'])+' assignmentId: '+last_pw_item['assignmentId']+' filename: '+last_pw_item['filename'])
                ai_ids_to_remove.append(last_pw_item['assignmentId']) 
                filenames.append(last_pw_item['filename']) 
        
        temp_array =[]

        for ai in self.all_ais :
            if(ai['id'] not in ai_ids_to_remove):
                temp_array.append(ai)
        
        for ai_id_to_remove in ai_ids_to_remove :
            del self.all_ais_pws_dict[ai_id_to_remove]
        
        self.all_ais = temp_array

    def repair_missing_ai_ids(self):

        converted_ai_id_dict = {}
        ais_to_add = {}

        for pw_ar in list(self.all_ais_pws_dict.values()):

            pw_item = self.get_first_none_start_pw(pw_ar)
            # so skip this ai ?
            if( (pw_item == None) or ("userInfo" not in pw_item) ):
                continue

            user_info_as_ai_id = pw_item["userInfo"]
            old_ai_id_as_key = pw_item["assignmentId"]
            filename = pw_item["filename"]

            all_db_ai_ids = [ ai["id"] for ai in self.all_ais]
            # candidate for USERINFO-fix ? (userInfo is exists as an ai id , and userInfo != ai_id  )
            if( (user_info_as_ai_id in all_db_ai_ids ) and 
                (user_info_as_ai_id != old_ai_id_as_key) ):
                
                converted_ai_id_dict[old_ai_id_as_key] = user_info_as_ai_id

            # not in DB ? (candidate for FileName-fix ?)
            if( (user_info_as_ai_id not in all_db_ai_ids ) and 
                (old_ai_id_as_key not in all_db_ai_ids) and 
                filename ):

                # filename matches filenames candidates ?
                file_name_to_dignoses_dict = self.get_file_name_to_dignoses_dict()
                if(filename in file_name_to_dignoses_dict.keys()):
                    ais_to_add[old_ai_id_as_key] = file_name_to_dignoses_dict[filename]

        # USERINFO-fix candidates:
        for old_ai_id in list(converted_ai_id_dict.keys()):
            if(old_ai_id in self.all_ais_pws_dict):
                new_ai_id = converted_ai_id_dict[old_ai_id]
                # remove old and place new while also fixing all pw_ar items to new ai_id
                pw_ar = self.all_ais_pws_dict[old_ai_id]
                
                # fix pw_ar
                for pw in pw_ar:
                    pw["assignmentId"] = pw["userInfo"]
                
                # fix dict:
                self.all_ais_pws_dict[new_ai_id] = pw_ar
                del self.all_ais_pws_dict[old_ai_id]
        
        # filename-fix candidates:
        for ai_id in list(ais_to_add.keys()):

            ai = {
                "id":ai_id,
                "diagnoses":ais_to_add[ai_id]
            }

            self.all_ais.append(ai)

    
    def get_file_name_to_dignoses_dict(self):
        return {
            "schizo.xlsx": [E_Diagnosis.schizophrenia.name],
            "moreschizo.xlsx": [E_Diagnosis.schizophrenia.name],
            "schizo_grid_txt.xlsx": [E_Diagnosis.schizophrenia.name],
            "260-311.xlsx" : [E_Diagnosis.benchmark.name],
            "312-378.xlsx" : [E_Diagnosis.benchmark.name],
            "379-394.xlsx" : [E_Diagnosis.benchmark.name],
            "394-407.xlsx" : [E_Diagnosis.benchmark.name],
            "420-432.xlsx" : [E_Diagnosis.benchmark.name],
            "440-454.xlsx" : [E_Diagnosis.benchmark.name],
            "455-467.xlsx" : [E_Diagnosis.benchmark.name],
            "474-491.xlsx" : [E_Diagnosis.benchmark.name],
            "501-553.xlsx" : [E_Diagnosis.benchmark.name],
            "554-594.xlsx" : [E_Diagnosis.benchmark.name],
            "595-652.xlsx" : [E_Diagnosis.benchmark.name],
            "653-678.xlsx" : [E_Diagnosis.benchmark.name],
            "679-694.xlsx" : [E_Diagnosis.benchmark.name],
            "695-742.xlsx" : [E_Diagnosis.benchmark.name],
            "743-773.xlsx" : [E_Diagnosis.benchmark.name],
            "774-811.xlsx" : [E_Diagnosis.benchmark.name],
            "812-829.xlsx" : [E_Diagnosis.benchmark.name],
            "831-840.xlsx" : [E_Diagnosis.benchmark.name],
            "841-857.xlsx" : [E_Diagnosis.benchmark.name],
            "858-873.xlsx" : [E_Diagnosis.benchmark.name],
            "874-891.xlsx" : [E_Diagnosis.benchmark.name],
            "892-902.xlsx" : [E_Diagnosis.benchmark.name],
            "903-923.xlsx" : [E_Diagnosis.benchmark.name],
            "924-945.xlsx" : [E_Diagnosis.benchmark.name],
            "946-969.xlsx" : [E_Diagnosis.benchmark.name],
            "969-987.xlsx" : [E_Diagnosis.benchmark.name],
            "988-1035.xlsx" : [E_Diagnosis.benchmark.name],
            "1chain.xlsx" : [E_Diagnosis.balls.name],
            "2chain.xlsx" : [E_Diagnosis.balls.name],
            "3chain.xlsx" : [E_Diagnosis.balls.name],
            "4chain.xlsx" : [E_Diagnosis.balls.name],
            "5chain.xlsx" : [E_Diagnosis.balls.name],
            "6chain.xlsx" : [E_Diagnosis.balls.name],
            "7chain.xlsx" : [E_Diagnosis.balls.name],
            "8chain.xlsx" : [E_Diagnosis.balls.name],
            "9chain.xlsx" : [E_Diagnosis.balls.name],
            "10chain.xlsx" : [E_Diagnosis.balls.name],
            "11chain.xlsx" : [E_Diagnosis.balls.name],
            "12chain.xlsx" : [E_Diagnosis.balls.name],
            "13chain.xlsx" : [E_Diagnosis.balls.name],
            "14chain.xlsx" : [E_Diagnosis.balls.name],
            "15chain.xlsx" : [E_Diagnosis.balls.name],
            "16chain.xlsx" : [E_Diagnosis.balls.name],
            "17chain.xlsx" : [E_Diagnosis.balls.name]
        }
    
    def debug_print_missing_ais(self):
        filenames_dict = {}
        found_filenames_dict= {}
        ai_list = []
        ais_found = 0
        for pw_ar in list(self.all_ais_pws_dict.values()):
            if( pw_ar and 
                pw_ar[0] and
                len(pw_ar[0]) > 2 
                ):
                
                pw_item = None
                for pw in pw_ar:
                    if(pw['word'] == 'start'):
                        continue
                    else:
                        pw_item = pw
                        break
                # no non start pw found ? so skip this ai
                if(pw_item == None):
                    continue
                
                
                ai_id = pw_item["assignmentId"]

                # is in DB?
                found = False
                
                for ai in self.all_ais:

                    if(ai['id']==ai_id):
                        ais_found = ais_found + 1
                        #print(ai_id)
                        found_filenames_dict[pw_item["filename"]] = True
                        found = True

                        break
                if(not found):
                    filenames_dict[pw_item["filename"]] = True
                    ai_list.append(ai_id)

        print('**************')     
        print('not found:'+str(len(ai_list)))
        print('found:'+str(ais_found))
        print('**************')

    def debug_print_possible_filenames(self):
        filename_dict = {}
        for pw_ar in list(self.all_ais_pws_dict.values()):
            if(pw_ar and pw_ar[0]):
                filename = pw_ar[0]["filename"]
                filename_dict[filename] = True
        #print(list(filename_dict.keys()))
        for fname in list(filename_dict.keys()):
            print(fname)


    def sortFilters(self,filters):

        ai_filters = []
        pw_filters = []
        for filter in filters:
            
            if  ((type(filter).__name__ is 'FilterLanguage') or
                (type(filter).__name__ is 'FilterCategory') ):
                pw_filters.append(filter)
            else:
                ai_filters.append(filter)  

        return (ai_filters,pw_filters)

    def create_participants(self):
        
        participants = [] 

        # filter to ais:
        ai_ids = self._filtered_pws.keys() 
        for ai_id in ai_ids:
            for ai in self.all_ais:
                if(ai_id == ai['id']):
                    ai['pws'] = self._filtered_pws[ai_id]
                    participant = Participant(ai)
                    participants.append(participant)

        return participants

    def generate_data_frame(self,participants):
        
        column_names = ['id','workerAge','gender','severity','meds','therapy','diagnoses','correctBallSelected']

        columns = {}

        for index,participant in enumerate(participants):

            ai_item = participant.data_item
            # iterate values in ai_items
            for col_name in column_names:
                
                value = None
                if((col_name in ai_item)):
                    value = ai_item[col_name]
                                
                if(value == None):
                    self.log_warnings += 1
                    log.warning('warning, assignment id:'+ai_item['assignmentId']+' missing:'+col_name)

                    value = np.nan

                if(index == 0):
                    columns[col_name] = [value]
                else:
                    columns[col_name].append(value)
                
        return  columns

    def get_filter(self,filters,column_name):
        for filter in filters:
            if(column_name == filter.column_name):
                return filter
        return None

    def filters_to_query(self,filters):
        # TBD: implement  E_FilterType.only_data_exists
        # TBD: implement filter if no_column 
        cat_filter =self.get_filter(filters,C_Attributes["category"])
        lang_filter =self.get_filter(filters,C_Attributes["language"])
        dig_filter =self.get_filter(filters,C_Attributes["diagnoses"])

        category =  cat_filter.data_item
        language = 'heb'
        if(lang_filter.data_item == "english"):
            language = 'en'

        # GSI1:
        diagnoses = dig_filter.data_items
        diagnosis_key = "UNKNOWN"
        if( diagnoses and 
            len(diagnoses) ):
            if(diagnoses[0] == "none"):
                diagnosis_key = "NONE"
            else:
                diagnosis_key = "EXISTS"

        GSI1PK = "CAT#"+category+"#LANG#"+language
        GSI1SK = "DIAG#" + diagnosis_key

        key_condition_expression = "#DDB_GSI1PK = :pkey and #DDB_GSI1SK = :skey"
        expression_attribute_values = {
            ":pkey": GSI1PK,
            ":skey": GSI1SK,
        }
        expression_attribute_names = {
            "#DDB_GSI1PK": "GSI1PK",
            "#DDB_GSI1SK": "GSI1SK",
        }
        filter_expression = ""
    
        # TBD: insert  the below into dignoses filter:
        if (diagnosis_key == "EXISTS"):
            eatn_key = "#DDB_"+C_Attributes["diagnoses"]
            expression_attribute_names[eatn_key] = C_Attributes["diagnoses"]

            for i,dig in enumerate(diagnoses):
                eatv_key = ":diagnoses_"+dig
                expression_attribute_values[eatv_key] = dig

                filter_expression = filter_expression + "contains("+ eatn_key + ","+ eatv_key+")"

                if i < (len(diagnoses)-1):
                    filter_expression = filter_expression + " AND "


        return  (key_condition_expression,expression_attribute_values,expression_attribute_names,filter_expression)

        

    def filter_v2(self,filters):
        cat_filter =self.get_filter(filters,C_Attributes["category"])
        lang_filter =self.get_filter(filters,C_Attributes["language"])
        dig_filter =self.get_filter(filters,C_Attributes["diagnoses"])

        # use GSI1 ? (cat/lang/diag)
        if(cat_filter and lang_filter and dig_filter):

            (key_condition_expression,
            expression_attribute_values,
            expression_attribute_names,
            filter_expression) = self.filters_to_query(filters)
            
            # DEBUG:
            print('querying GSI1:'+str(key_condition_expression)+' for: '+str(dig_filter.data_items))
            print(str(key_condition_expression))
            print(str(expression_attribute_values))
            print(str(expression_attribute_names))
            print(str(filter_expression))

            table = self.db_instance.Table("AmarilDataSandbox") # pylint: disable=no-member

            response = table.query(
                TableName="AmarilDataSandbox",
                IndexName="GSI1PK-GSI1SK-index",
                KeyConditionExpression=key_condition_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ExpressionAttributeNames=expression_attribute_names,
                #Limit=100,
                FilterExpression=filter_expression,
            )

            return response["Items"]

        # scan:
        else:
            print('Scna not imple,mented yet')
        
        return None

    def create_participants_v2(self,data):
        participants = [] 


        for ai in data:
            
            ai['pws'] = []
            participant = Participant(ai)
            participants.append(participant)

        return participants



    def filter(self,filters):

        # sanity :
        if((not self.all_ais_pws_dict ) or (not self.all_ais ) ):
            print("Error, trying to filter when data is not loaded.")
            return False

        (ai_filters,pw_filters) = self.sortFilters(filters)
        self._filtered_ais_dict = self.filter_ais(ai_filters)
        self._filtered_pws = self.filter_pws(pw_filters,self._filtered_ais_dict)
        
        return True
 




    
    def db_get_index_items(self,language,category):
        table = self.db_instance.Table("Index") # pylint: disable=no-member
        fe = Attr('category').eq(category) & Attr('language').eq(language)
        pe = "#cat, #indx,#trans,#lang,id,word"
        ean = { "#cat": "category","#indx": "index", "#trans": "transform", "#lang": "language"}

        response = table.scan(
            FilterExpression=fe,
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
            )

        items = response['Items']
        
        while response.get('LastEvaluatedKey'):
            response = table.scan( FilterExpression=fe,
                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])        

        return items

    def db_get_ais(self):
        ais_ean = {   
            "#cat": "category", 
            "#lang": "language",
            "#crdAt" : "createdAt"
        }
        ais_pe = "#cat,#lang,id,#crdAt,diagnoses,multiAnswer,secondsRecorded,secondsRequested,workerAge,gender,severity,meds,therapy,filename,experimentName,correctBallSelected"

        all_ais = self.db_get_all_items(ais_pe,
                                                ais_ean,
                                                C_AI_Table)
        print("loaded ais items:"+str(len(all_ais)))

        # noremelize diagnoses:
        dig_normelizer =  NormelizeDiagnoses()
        meds_normelizer =  NormelizeMeds()
        for index,ai in  enumerate(all_ais): 
            ai = dig_normelizer.normelize(ai)
            ai = meds_normelizer.normelize(ai)
        
        return all_ais
       

    def db_get_pws(self):
        pws_pe = "#cat,#lang, #indx,#wrd,id,#strt,totalDuration,filename,assignmentId,#end,userInfo" 
        pws_ean = { "#cat": "category","#wrd":"word","#indx": "index","#strt":"start","#end":"end", "#lang": "language" }

        all_pws = self.db_get_all_items(pws_pe,
                                        pws_ean,
                                        C_PW_Table)
        # ais_dict is a dictionary in which key = ai_id , value = array of the ai's product words
        all_ais_pws_dict = self.transform_pws_and_sort_to_ais(all_pws)

        return all_ais_pws_dict


    def db_get_indexes(self):
        categories = ["animals"]
        languages = ["hebrew", "english"]

        # get index items:
        indexes = {}
        for category in categories:
            for language in languages:
                index_items = self.db_get_index_items(language,category)
                indexes[category+"_"+language] = index_items

        return indexes

    def get_fe(self,filter_object):

        fe = None
        for attr_key in filter_object:

            afe = filter_object[attr_key]
            
            # chain afe s (first afe?):
            if(not fe):
                fe = afe 
            else:
                fe = fe & afe 
        
        return fe

    def db_get_items(self,filter_obj,pe,ean_dict,table_name):
        table = self.db_instance.Table(table_name) # pylint: disable=no-member

        fe = self.get_fe(filter_obj)
        if(not fe):
            fe = {}

        response = table.scan(
            FilterExpression=fe,
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean_dict
        )

        items = response['Items']

        while response.get('LastEvaluatedKey'):
            
            

            response = table.scan( FilterExpression=fe,
                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean_dict,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])  

        return items

    def to_output_by_attr(self,ais_dict,attr):
        count = 0
        output_ar = []
        for ai_id in ais_dict.keys():
            pw_ar = ais_dict[ai_id]
            if(pw_ar == None):
                #print("no product words for assignment:"+ai_id+" skipping ...")
                count = count + 1
                continue
            
            sorted_pw_ar = sorted(pw_ar, key=lambda k: k['start'])

            
            if(attr == C_Attributes["start"]):
                atrr_ar = [ float(pw[attr])  for pw in sorted_pw_ar]
                del atrr_ar[0]
                output_ar.append(atrr_ar)
            
            if(attr == C_Attributes["index"]):
                atrr_ar = [  int(pw[attr])  for pw in sorted_pw_ar]
                del atrr_ar[0]
                output_ar.append(atrr_ar)

            
        #print("no product words for assignments:"+str(count))
        return output_ar
    
    def filter_ais(self,filters):

        filtered_ais = []
        for ai in  self.all_ais:
            passed = True
            for filter in filters:
                if(not filter.passed(ai,self.all_ais_pws_dict)):
                    passed = False
                    break
            
            if(passed):
                filtered_ais.append(ai)
                                                
        # empty ?
        if(not len(filtered_ais)):
            return ({})

        # filter to ais:
        ai_ids = [ai["id"] for ai in filtered_ais]
        filtered_ais_dict = {k: self.all_ais_pws_dict.get(k, None) for k in ai_ids}

        return filtered_ais_dict

    def get_db_pw_items(self,filter_obj):
        # get Assignment's ids :
        ais_filter_object = filter_obj
        ais_ean = {   
            "#cat": "category", 
            "#lang": "language",
            "#crdAt" : "createdAt"
        }
        ais_table_name = C_AI_Table
        ais_pe = "#cat,#lang,id,#crdAt"

        ais = self.db_get_items(ais_filter_object,
                                    ais_pe,
                                    ais_ean,
                                    ais_table_name)
                                                
        # empty ?
        if(not len(ais)):
            return ([])

        ai_ids = [ai["id"] for ai in ais]
        
        max_num = 100
        pws = []
        for indx,aiid in enumerate(ai_ids):
            mod = indx % max_num
            if(mod == 0):
                #time.sleep(10)
                sub_ids = ai_ids[indx:(indx+max_num)]
                # check end :
                if((indx+max_num) > len(ai_ids) ):
                    sub_ids = ai_ids[indx:len(ai_ids)]
                    print('sending batch for ai_ids indexs: '+str(indx)+':'+str(len(ai_ids))) 
                else:
                    print('sending batch for ai_ids  indexs: '+str(indx)+':'+str(indx+max_num)) 

                # Get Product Words that belong to these assignments:
                pws_filter_object = {
                    "assignmentId" : Attr("assignmentId").is_in(sub_ids)
                }

                pws_pe = "#cat,#lang, #indx,#wrd,id,#strt,totalDuration,filename,assignmentId,#end,userInfo" 
                pws_ean = { "#cat": "category","#wrd":"word","#indx": "index","#strt":"start","#end":"end", "#lang": "language" }

                # concat batches
                new_pws = self.db_get_items(pws_filter_object,
                                            pws_pe,
                                            pws_ean,
                                            C_PW_Table)
                pws.extend(new_pws) 
        return pws

    def get_index_item(self,items,word):
        for item in items:
            if( item["word"] == word ):
                return item
        return None

    def transform_pws_and_sort_to_ais(self,pw_items):

        # sanity:
        if pw_items and len(pw_items) > 0:
            #print('transform_product_words recieved'+str(len(pw_items))+'pw items')
            print('')
        else:
            print('error, transform_product_words no pw items recieved.')
            return []

        ais_dict = {}
        for pw_item in pw_items:
            # sanity:
            if  ((not ("category" in pw_item.keys())  ) or 
                (not pw_item["category"] ) or 
                (not ("language" in pw_item.keys())  ) or 
                (not pw_item["language"] )) : 
                
                print('warning,transform_product_words : skipping to next pw item since did not have language and category columns.')
                continue

            # get index_items for specific pw_item (by language and category)
            index_items = self.indexes[pw_item["category"]+"_"+pw_item["language"]]
            if (not index_items ) or not len(index_items):
                print('error, transform_product_words: no Index items found.')
                return []
            # swap indexID for number:
            index_item = self.get_index_item(index_items,pw_item["word"])
            if(not index_item):
                print('Error, item for '+pw_item["word"]+' doesnt exist index will be empty ...')
                pw_item.pop("index")
                continue
            else:
                # is transform ?
                if( (not ("index" in index_item) ) and ("transform" in index_item) ):
                    transform_item = self.get_index_item(index_items,index_item["transform"])
                    if(not transform_item):
                        print('Error, item for '+index_item["transform"]+' doesnt exist index will be empty ...')
                        pw_item.pop("index")
                        continue
                    pw_item["index"] = transform_item["index"]
                    pw_item["word"] = transform_item["word"]
                    pw_item["originalWord"] = index_item["word"]
                else:
                    pw_item["index"] = index_item["index"]

            # sort into ai dicts:
            ai_ar = []
            # assignmentId present ?
            if not("assignmentId" in pw_item.keys()):
                continue
            ai_id = pw_item["assignmentId"]
            if(ai_id in ais_dict.keys()):
                ai_ar = ais_dict[ai_id]
            ai_ar.append(pw_item)
            ais_dict[ai_id] = ai_ar


        return ais_dict

    def db_get_all_items(self,pe,ean_dict,table_name):
        table = self.db_instance.Table(table_name) # pylint: disable=no-member
        fe = Attr('isSandbox').ne(True) 

        response = table.scan(
            FilterExpression=fe,

            ProjectionExpression=pe,
            ExpressionAttributeNames=ean_dict
        )

        items = response['Items']

        while response.get('LastEvaluatedKey'):
            
            response = table.scan( 
                                    FilterExpression=fe,

                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean_dict,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])  

        return items
