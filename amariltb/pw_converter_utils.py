from openpyxl import Workbook

Languages = {
       "hebrew":"hebrew",
       "english":"english"
}

def rev(s,langauge): 
    if(langauge == Languages["hebrew"]):
        return s[::-1]
    else:
        return s 

def normelize_english_remove_a_an(normelized_word):
    # check "a_":
    first_2_chars = normelized_word[0:2]
    if first_2_chars == "a_":
        normelized_word = normelized_word[2:]
    
    # check "an_":
    first_3_chars = normelized_word[0:3]
    if first_3_chars == "an_":
        normelized_word =  normelized_word[3:]

    return normelized_word
"""
def normelize_english_to_singular(normelized_word):
    # plural to singular:
    
    #Note:
    #we replace all underscore to spaces, so that singularization works properly in case in which we recieved an already underscored word.
    
    p = inflect.engine()
    normelized_word = normelized_word.replace('_', ' ')
    singular_normelized_word = p.singular_noun(normelized_word)
    if not singular_normelized_word:
        return normelized_word
    
    return singular_normelized_word
"""


def normelize_word(word,language,category):
    if(language == Languages["english"]):
                
        # clean - remove leading and trailing spaces:
        normelized_word = word.strip()
        
        # lower:
        normelized_word = normelized_word.lower()

        # plural to singular:
        #normelized_word = normelize_english_to_singular(normelized_word)

        # underscore:
        normelized_word = normelized_word.replace(' ','_')

        # remove preceding a / an:
        normelized_word = normelize_english_remove_a_an(normelized_word)

    if(language == Languages["hebrew"]):
        # clean - remove leading and trailing spaces:
        normelized_word = word.strip()
        
        # underscore:
        normelized_word = normelized_word.replace(' ','_')
        # reverse ? TBD
        #normelized_word =  rev(normelized_word,language)

    return normelized_word

def update_sheet(filename,ar):
    wb = Workbook()
    ws = wb.active 
    ws.title = filename
    dest_filename = filename

    for item in ar:
        row = []
        for key in item:
            row.append(item[key])
        ws.append(row)
    wb.save(filename = dest_filename)
    return


def update_index_sheet(filename,ar):
    wb = Workbook()
    ws = wb.active 
    ws.title = filename
    dest_filename = filename

    for item in ar:
        row = []
        row.append(item['category'])
        row.append(item['language'])
        row.append(item['id'])
        row.append(item['word'])
        if("index" in item.keys()):
            row.append(item['index'])
        else:
            row.append('')
        if("transform" in item.keys()):
            row.append(item['transform'])
        else:
            row.append('')

        ws.append(row)
    wb.save(filename = dest_filename)
    return
