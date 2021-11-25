
from amaril_data import AmarilData
from amaril_data_normelizer import E_Diagnosis 
from amaril_data_filters import E_FilterType,FilterPWWordCount,FilterDiagnoses,FilterLanguage,FilterCategory,FilterNumberInRange,FilterStringEquals,FilterBool
import pandas as pd

duration_filter = FilterNumberInRange(55,65,'secondsRecorded')
age_filter = FilterNumberInRange(35,45,'workerAge')
gender_filter = FilterStringEquals('male','gender')
dig_filter = FilterDiagnoses( [E_Diagnosis.adhd,E_Diagnosis.ptsd],E_FilterType.also_data_exists)
lang_filter = FilterLanguage('english')
category_filter = FilterCategory('animals')  
meds_filter = FilterBool(False,'meds')
therapy_filter = FilterBool(True,'therapy')
severity_filter = FilterNumberInRange(1,2,'severity')
pw_count_filter = FilterPWWordCount(1)

filters  = []#[meds_filter,duration_filter,age_filter,lang_filter,category_filter]
#filters  = [therapy_filter,dig_filter,meds_filter]

# Create Data:
amaril_data = AmarilData(filters,False)
#amaril_data.hidrate_participants_audio_segments()

# 1. Index:
index = amaril_data.get_index('animals','english')
#print(index)

# 2. Create Pandas Data frame: 
data_frame = pd.DataFrame(amaril_data.data_frame_dict)
#print (data_frame)
#data_frame.to_csv (r'./dataframe.csv', index = False, header=True)

# test ts:
output_start_attribute = []
output_index_attribute = []
assignment_ids = {}
for participant in amaril_data.participants:
    for pw in participant.data_item['pws']:
        if(pw['start']>66):
            assignment_ids[pw['assignmentId']] = True
for ai_id in assignment_ids.keys():
    print(ai_id)

#print(output_index_attribute)
#print(output_start_attribute)

# 4. Audio segment:
"""
for participant in amaril_data.participants:
    if( participant and participant.audio_segment):
        print('audio duration for  participant : '+participant.data_item['id'] +'is: ',
                participant.audio_segment.duration_seconds)
"""
