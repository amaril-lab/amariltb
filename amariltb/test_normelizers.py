from amaril_data_normelizer import E_Diagnosis,NormelizeMeds,NormelizeDiagnoses,NormelizeCreateDiagnosesDict

from amaril_data_filters import  FilterBool,FilterDiagnoses,E_FilterType,FilterNumberInRange



meds_normelizer =  NormelizeMeds()
ai = {  "multiAnswer":
        '{\"diagnoses\":[\"Depression\",\"Anxiety or panic attacks\",\"General Anxiety Disorder (GAD)\",\"Bipolar disorder\"],\"meds\":\"no\"}'
    
    }
ai = meds_normelizer.normelize(ai)
print(ai)


"""
dig_normelizer =  NormelizeDiagnoses()
ai = {  "multiAnswer":
        '{\"diagnoses\":[\"Depression\",\"Anxiety or panic attacks\",\"General Anxiety Disorder (GAD)\",\"Bipolar disorder\"],\"meds\":\"yes\"}'
        ,"diagnoses":
        'ADHD (הפרעת קשב, ריכוז והיפראקטיביות),Generalized Anxiety Disorder (GAD)'
    }
ai = dig_normelizer.normelize(ai)
print(ai)
"""
########
"""
normelizer =  NormelizeCreateDiagnosesDict()
#ai = { "diagnoses":'הפרעות אכילה(אנורקסיה ו/או בולימיה),PTSD (הפרעת דחק פוסט-טראומטית),ADHD (הפרעת קשב, ריכוז והיפראקטיביות)'
#    }
ai = { "diagnoses":'Bipolar Disorder,Alcoholism'}
digs_dict = {}
digs_dict = normelizer.update_dict(ai,digs_dict)
print(digs_dict)
"""
# prepare digs dict:
"""
normelizer = NormelizeCreateDiagnosesDictA()
digs_dict = {}
for ai in self.all_ais:

    digs_dict = normelizer.update_dict(ai,digs_dict)

for key in digs_dict.keys():
    print(key)
"""
######## TEST FILTER ##########

"""
dig_filter = FilterDiagnoses([E_Diagnosis.add,E_Diagnosis.adhd],E_FilterType.only_data_exists)

ai = {  "diagnoses":['add','adhd','ptsd']}
result = dig_filter.passed(ai)
print(result)
"""
"""
duration_filter = FilterNumberInRange(55,65,'secondsRecorded')

ai = {  "secondsRecorded":54}
result = duration_filter.passed(ai)
print(result)
"""
"""
meds_filter = FilterBool(False,'meds')

ai = {  "meds":True}
result = meds_filter.passed(ai)
print(result)
"""
