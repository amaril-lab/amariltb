#!/usr/bin/env python3

from openpyxl import load_workbook
import os
from decimal import Decimal

# prrat parser input types:
"""
'typeA' info is before start (and if file exists before start then info before file): 
- info ("עודד-ז-26-14") interval and then 'start' interval (example: 'inputs/101-114.xlsx') or 
  info ("עודד-ז-26-14") interval then 'file' interval and then 'start' interval (example: 'inputs/1-63.xlsx')
- info is in an interval between 'file' and 'start' (example 501-553.xlsx):
- 
"""

def rev(s,heb): 
    if(heb) and not (s == 'start'):
        return s[::-1]
    else:
        return s 

class NPrratGridParser:
    def __init__(self,language):
        self.language = language

    def getNParser(self,data):
        file_type = self.identifyFileType(data)
        
        if(file_type == 'typeA'):
            parser = PrratParserTypeA(self.language)
            return parser
        return False

    def identifyFileType(self,data):
        file_type = 'typeA'
        for index,item in enumerate(data):
            
            if item["text"] == 'start':
                #prev_item = data[index-1]
                prev_prev_item =  data[index-2]
                if (prev_prev_item):
                    pass
                    #if ('file' in prev_item["text"]):
                    #    file_type = 'typeB'


        return file_type

    def parse(self,filename):

        participant_data = []
        #wb = load_workbook(filename)
        #index_sheet =  wb.worksheets[0]
        participant_id = ""
        text = ''
        xmin=''
        xmax = ''
        with open(filename, "r",encoding= 'unicode_escape') as a_file:
            for line in a_file:
                row_val = line.strip()
                # xmin:
                xmin_start_indx = row_val.find('xmin =')
                if(xmin_start_indx != -1):
                    xmin = row_val[xmin_start_indx + 6 :]
                    xmin = xmin.strip()

                # xmax:
                xmax_start_indx = row_val.find('xmax =')
                if(xmax_start_indx != -1):
                    xmax = row_val[xmax_start_indx + 6 :]
                    xmax = xmax.strip()
                
                # text:
                text_start_indx = row_val.find('text =')
                if(text_start_indx != -1):
                    text = row_val[text_start_indx + 6 :]
                    text = text.replace('"','').strip()
                    text = text.replace('“','').strip()

                if(xmin == '' or xmax == '' or text == ''):
                    continue

                if(len(text)  ):
                    item = {
                        "participant_id":participant_id ,#rev(participant_id,self.language == 'hebrew'),
                        "xmin":xmin,
                        "xmax":xmax,
                        "text":text #rev(text,self.language == 'hebrew')
                    }
                    participant_data.append(item)
                        
                text = ''
                xmin=''
                xmax = ''   
                
        return participant_data


class NParser:
    def __init__(self,language):
        pass 

# just one interval with user name befor 'start'
class PrratParserTypeA(NParser):
    def __init__(self,language):
        NParser.__init__(self,language)
    
    def parse_users(self,data):
        current_user_info = ''
        current_file_start = "0.0"
        for index,item in enumerate(data):
            # found 'start' ?
            if item["text"] == 'start':
                # update current file start:
                current_file_start = item["xmin"]
                prev_item = data[index-1]
                if (prev_item):  
                    # previous is 'file' ?
                    
                    if(prev_item["text"] == 'file'):
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
            item["participant_id"] = current_user_info

        # remove items with text=="file" and items with participant_id=="remove"
        data = [item for item in data if (not (item["participant_id"] == 'remove') ) and (not (item["text"] == 'file') ) ]
       
        return data
        