from .pw_converter_input_normelizer_parser import NPrratGridParser 
from .pw_converter_input_normelizer_filters import NSonixTranscriptErrorsFilter,NPrratUserInfoFilter
import os
import json
from decimal import Decimal
import ntpath 
import pathlib

# TBD: make global const file
Categories = {
    "animals":"animals",
    "food":"food",
    "plants":"plants", 
    "transport":"transport", 
    "drinks" : "drinks"
}

Languages = {
    "hebrew":"hebrew",
    "english":"english"
}

def createRecordingData(items,filename,assignmentId,language):

    rec_data = {
        'assignmentId': assignmentId,
        'language':language,
        'items':items
    }
    if(not (filename == None) ):
        rec_data["filename"] = filename
    
    return rec_data


def keyExists(dict, key):   
    if key in dict.keys(): 
        return True 
    else: 
        return False

def create_converter_input_data(filename,data,language,user_category):
    # (sonix) override filename from item ?
    override_filename = False 
    if( filename == None ):
        override_filename = True

    # validate language:
    if(language not in  Languages.values() ):
        raise NameError('Language which was entered does not exist in languages.')

    # validate category:
    if(user_category not in  Categories.values() ):
        raise NameError('Category which was entered does not exist in categories.')

    json_data = {'recordings':[]}
    # data sorted by filename:
    recordings_dict = {}
    for data_item in data :

        # TBD: move to RemoveTextItemsFilter - dont write the starts:
        if(data_item['text'] == 'start'):
            data_item['xmin'] = '0'
            data_item['xmax'] = '0'


        # override filename (sonix case):

        if ( override_filename and ('filename' in data_item) ):
            filename = data_item['filename']

          
        json_item = {
            #'filename':filename,
            'word':data_item['text'],
            'start':float(round(Decimal(data_item['xmin']),3)),
            #'end':float(round(Decimal(data_item['xmax']),3)),
            #'category':user_category
        }

        if('xmax'  in data_item):
            json_item['end']=float(round(Decimal(data_item['xmax']),3)),


        # propogate file durtion:
        if(keyExists(data_item,'file_duration')):
            json_item['file_duration'] =  data_item['file_duration']

        key = data_item['participant_id']
        # create array if does not exist:
        if(keyExists(recordings_dict, key)):
            ar = recordings_dict[key]
            ar.append(json_item)
            recordings_dict[key] = ar 
        else:
            ar = []
            ar.append(json_item)
            recordings_dict[key] = ar

    # dict to ar:
    for participant_id in recordings_dict.keys():
        items = recordings_dict[participant_id]
        assignmentId = os.path.splitext(participant_id)[0]
        if(override_filename and items and len(items) and ('filename' in items[0])):
            filename = items[0]['filename']
        #recording_data = createRecordingData(items,filename,assignmentId,language= language)
        recording_data = {
            'assignmentId': assignmentId,
            'pws':items,
            'pwsCount':len(items)
        }
        json_data['recordings'].append(recording_data)

    return json_data

def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def normelize_prrat_grid(grid_file_xlsx,language,category):

    # get data :
    parser = NPrratGridParser(language)
    data = parser.parse(grid_file_xlsx)
    
    # filters:
    user_info_filter = NPrratUserInfoFilter(language)
    data = user_info_filter.filter(data)

    grid_filename = path_leaf(grid_file_xlsx)
    input_data = create_converter_input_data(grid_filename,data,language,category)

    return input_data

def normelize_prrat(filename,language,category):

    # normelize prrat file:
    data = normelize_prrat_grid(filename,language,category)
    
    # create JSON Input:
    current_path = pathlib.Path().absolute()
    debug_dir =  os.path.join(current_path, 'debug')
    # create directory for debug:
    if(not os.path.isdir(debug_dir)):
        os.mkdir(debug_dir)
    debug_target_filename = os.path.join( debug_dir , 'converterInput.json')

    with open(debug_target_filename, 'w', encoding='utf8') as json_file:
        json.dump(data, json_file, indent=4, ensure_ascii=False)
    
    return data 

def normelize_sonix_data(data,language,category):

    # filters:
    transcript_error_filter = NSonixTranscriptErrorsFilter(language)
    data = transcript_error_filter.filter(data)

    data = create_converter_input_data(None,data,language,category)

     # create JSON Input:
    current_path = pathlib.Path().absolute()
    debug_dir =  os.path.join(current_path, 'debug')
    # create directory for debug:
    if(not os.path.isdir(debug_dir)):
        os.mkdir(debug_dir)
    debug_target_filename = os.path.join( debug_dir , 'converterInput.json')
   
    with open(debug_target_filename, 'w', encoding='utf8') as json_file:
        json.dump(data, json_file, indent=4, ensure_ascii=False)

    return data 

