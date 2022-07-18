import logging
log = logging.getLogger(__name__)


class Participant:

    def __init__(self,data_item,index):
        self.audio_segment = None
        self.init(data_item,index)

    def init(self,data_item,index):
        
        pw_ar = data_item['pws']
        
        # sanity:
        if(pw_ar == None or len(pw_ar) == 0):
            log.warning('Participant:' + data_item['assignmentId']+ ' has no product words.')
        
        # sort pws by start ts :    
        sorted_pws_by_start_ts = sorted(pw_ar, key=lambda k: k['start'])
        # filter 'start' items:
        filtered_sorted_pws_by_start_ts = []
        for pw_item in sorted_pws_by_start_ts:
            pw_item['index'] = index[pw_item["word"]]
                
            filtered_sorted_pws_by_start_ts.append(pw_item)
        
        data_item['pws'] = filtered_sorted_pws_by_start_ts
        self.data_item = data_item

        self.pws_start_tss = [ float(pw['start'])  for pw in data_item['pws']]
        self.pws_indexes = [  int(pw['index'])  for pw in data_item['pws']]
        


    def get_audio_gs_full_path(self):
        if('filename' in self.data_item):
            return self.data_item['filename']

        log.warning('get_audio_gs_full_path participant:' + self.data_item['id']+ ' has no filename.')
        return None

    def set_audio_segment(self, audio_segment):
        self.audio_segment = audio_segment
