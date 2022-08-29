from enum import Enum

C_Attributes = {
    "isSandbox":"isSandbox",
    "diagnoses":"diagnoses",
    "meds":"meds",
    "category":"category",
    "secondsRecorded":"secondsRecorded",
    "secondsRequested":"secondsRequested",
    "language":"language",
    "createdAt":"createdAt",
    "index":"index",
    "start":"start",
    "experimentName":"experimentName",
}

class E_FilterType(Enum):
    also_data_exists = 0
    only_data_exists = 1

class Filter:
    def passed(self,item,all_ais_pws_dict = None):
        pass

class FilterPWWordCount(Filter):
    def __init__(self,min):
        self.min = min
    
    def passed(self,item_dict,all_ais_pws_dict = None):
        if( all_ais_pws_dict and 
            item_dict['id'] in all_ais_pws_dict.keys()):
            
            pw_count = len(all_ais_pws_dict[item_dict['id']]) - 1
            return (pw_count >= self.min)
        
        
        return not (self.min > 0)

class FilterNumberInRange(Filter):
    def __init__(self,min,max,column_name,filter_if_no_column = True):
        self.column_name  = column_name
        self.min = min
        self.max = max
        self.filter_if_no_column = filter_if_no_column
    
    def passed(self,item_dict,all_ais_pws_dict = None):

        # no column:
        if not (self.column_name in item_dict ): 
            # should pass?
            if(self.filter_if_no_column): 
                
                return False
            else:
            
                return True
            
        num  = item_dict[self.column_name]
        
        if( (self.min <= num) and 
                  (num <= self.max) ):
            return True
        else:
            return False


class FilterBool(Filter):
    def __init__(self,bool_val,column_name,filter_if_no_column = True):
        self.column_name  = column_name
        self.bool_val = bool_val
        self.filter_if_no_column = filter_if_no_column
    
    def passed(self,item_dict,all_ais_pws_dict = None):

        # no column:
        if not (self.column_name in item_dict ): 
            # should pass?
            if(self.filter_if_no_column): 
                
                return False
            else:
            
                return True
            
        bool_val  = item_dict[self.column_name]
        
        return (bool_val == self.bool_val)

class FilterString(Filter):
    def __init__(self,str_val,column_name,filter_if_no_column = True):
        self.column_name  = column_name
        self.str_val = str_val
        self.filter_if_no_column = filter_if_no_column
    
    def passed(self,item_dict,all_ais_pws_dict = None):

        # no column:
        if not (self.column_name in item_dict ): 
            # should pass?
            if(self.filter_if_no_column): 
                
                return False
            else:
            
                return True
            
        str_val  = item_dict[self.column_name]
        
        return (str_val == self.str_val)

class FilterStringEquals(Filter):
    def __init__(self,str,column_name,filter_if_no_column = True):
        self.column_name  = column_name
        self.str = str
        self.filter_if_no_column = filter_if_no_column
    
    def passed(self,item_dict,all_ais_pws_dict = None):

        # no column:
        if not (self.column_name in item_dict ): 
            # should pass?
            if(self.filter_if_no_column): 
                
                return False
            else:
            
                return True
            
        val  = item_dict[self.column_name]
        
        if( self.str == val):
            return True
        else:
            return False


class FilterStringIsContainedBy(Filter):
    def __init__(self,str,column_name,filter_if_no_column = True):
        self.column_name  = column_name
        self.str = str
        self.filter_if_no_column = filter_if_no_column
    
    def passed(self,item_dict,all_ais_pws_dict = None):

        # no column:
        if not (self.column_name in item_dict ): 
            # should pass?
            if(self.filter_if_no_column): 
                
                return False
            else:
            
                return True
            
        val  = item_dict[self.column_name]
        
        if( self.str in val):
            return True
        else:
            return False



class FilterColumnDoesExist(Filter):
    def __init__(self,column_name,does_exist):
        self.column_name  = column_name  
        self.does_exist  = does_exist    
  
    
    def passed(self,item_dict,all_ais_pws_dict = None):

        # no column:
        return (self.column_name in item_dict ) == self.does_exist



class FilterLanguage(Filter):
    def __init__(self,data_item,column_name = 'language'):
        self.column_name  = column_name
        self.data_item = data_item
    
    def passed(self,item_dict,all_ais_pws_dict = None):

        # no column:
        if not (self.column_name in item_dict ): 
            return False
            
        if(item_dict[self.column_name] == self.data_item):
            return True
        else:
            return False



class FilterCategory(Filter):
    def __init__(self,data_item,column_name='category'):
        self.column_name  = column_name
        self.data_item = data_item
    
    def passed(self,item_dict,all_ais_pws_dict = None):

        # no column:
        if not (self.column_name in item_dict ): 
            return False
            
        if(item_dict[self.column_name] == self.data_item):
            return True
        else:
            return False

class FilterDiagnoses(Filter):
    def __init__(self,data_items,filter_type,filter_if_no_column = True,column_name = C_Attributes["diagnoses"]):
        self.column_name  = column_name
        self.filter_type = filter_type
        self.filter_if_no_column = filter_if_no_column
        # convert data items from enum to string:
        self.data_items = [d_item.name for d_item in data_items]
    
    def passed(self,item_dict,all_ais_pws_dict = None):

        # no column ?
        if not (self.column_name in item_dict ): 
            
            # should pass?
            if(self.filter_if_no_column): 
                return False
            else:
                return True

        # column exists
        column_value_list = item_dict[self.column_name]
        # no digs ?
        if(len(column_value_list) == 0):
            # should pass?
            if(self.filter_if_no_column): 
                return False
            else:
                return True

        found_all = True
        for input_item_val in self.data_items:
            
            
            exist_in_column_val = False
            for column_val_item in column_value_list:
                if(column_val_item == input_item_val):
                    exist_in_column_val = True
                    break

            if(not exist_in_column_val):
                found_all = False
                break
    
        if( self.filter_type == E_FilterType.also_data_exists):
            return found_all
        
        if( self.filter_type == E_FilterType.only_data_exists):

            return ( found_all and 
                    ( len(self.data_items) == len(column_value_list) )  )   

        # defualt = dont filter.    
        return True
