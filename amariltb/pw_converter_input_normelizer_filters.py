import uuid
from decimal import Decimal
import hashlib
from .pw_converter_db import DynamoDB
import os
NORMELIZER_TRANSCRIPT_WORD_ERROR_FLAG = '0'

class NormelizerFilter:
    def __init__(self,language):
        pass


# just one interval with user name befor 'start'
class NPrratUserInfoFilter(NormelizerFilter):
    def __init__(self,language):
        NormelizerFilter.__init__(self,language)
    
    def filter(self,data):
        current_user_info = ''
        current_file_start = "0.0"
        for index,item in enumerate(data):
            # found 'start' ?
            if 'start' in item["text"].lower():

                # update current file start:
                current_file_start = item["xmin"]
                prev_item = data[index-1]
                if (prev_item):  
                    # previous is 'file' ?
                    if('file' in prev_item["text"].lower()):
                        # remove it:
                        prev_item["participant_id"] = 'remove'

                        # skip one backwards:
                        prev_prev_item = data[index-2]
                        if (prev_prev_item):
                            # assign this item as data:
                            current_user_info =  prev_prev_item["text"]
                            prev_prev_item["participant_id"] = 'remove'
                    
                    else:
                        current_user_info =  prev_item["text"]
                        prev_item["participant_id"] = 'remove'
                

            
            item["xmin"] = str( Decimal(item["xmin"]) - Decimal(current_file_start) )
            item["xmax"] = str( Decimal(item["xmax"]) - Decimal(current_file_start) )

            
            # sanity userInfo:
            if(current_user_info == ''):
                current_user_info ='remove'

            item["participant_id"] = current_user_info
            # TBD: temporarilty hard code file duration (until we can get it from assignment)
            item["file_duration"] = 60



        # remove items with text=="file" and items with participant_id=="remove"
        data = [item for item in data if (not (item["participant_id"] == 'remove') ) and (not (item["text"] == 'file') ) ]
       
        return data
        


class NSonixTranscriptErrorsFilter(NormelizerFilter):
    def __init__(self,language):
        pass 

    # TBD: move all create methods into db
    def create_transcript_error_item(self,item):
         
        #{'participant_id': '695', 'xmin': '2.4678760627118714', 'text': 'פיל'}
        assignmentId = item["participant_id"]
        word = item["text"]
        start = round(Decimal(item["xmin"]),3)
        #filename = item["filename"]


        id = assignmentId + str(start) + word
        hashed_id = hashlib.sha1(id.encode("UTF-8")).hexdigest()[5:]
        new_error_word = {
            "id" : hashed_id,
            "word" : word,
            "start" : start,
            #"filename" : filename,
            "assignmentId" : os.path.splitext(assignmentId)[0]

        }
        
        return new_error_word
        
        

    def document_transcript_error(self,item):
        print('document_transcript_error:',item)
        # document error :
        new_transcript_error = self.create_transcript_error_item(item)
        db = DynamoDB()
        db.update_transcript_errors([new_transcript_error])

    def filter(self,data):
        for item in data:
            # TBD: support for heberw might reverse order:
            word = item['text']
            #first_char = word[0]
            flag_index = word.find(NORMELIZER_TRANSCRIPT_WORD_ERROR_FLAG)
            # clean the flag:
            if(not (flag_index == -1)):
                if(flag_index == 0):
                    item['text'] = word[1:]
                if(flag_index == (len(word)-1 ) ):
                    item['text'] = word[:len(word)-1 ]
                
                self.document_transcript_error(item)

        return data