import json

from enum import Enum

class E_Diagnosis(Enum):
    none = 0
    anxiety_panic_attacks = 1
    gad = 2
    alcoholism = 3
    eating_disorders = 4
    ptsd = 5
    schizophrenia = 6
    bipolar_disorder = 7
    adhd = 8
    add = 9
    ocd = 10
    depression = 11
    benchmark = 12
    balls = 13
class NormelizeAIBase:


    def normelize(self):
        pass

class NormelizeDiagnoses(NormelizeAIBase):
        
    def find_comma_in_pths(self,str,start_index=0):
        # "(" ?
        if( (str.find('(') != -1) ):
            open_p_index = str.find('(') 
            
            # "," ?
            if( (str.find(',') != -1) ):
                com_index = str.find(',') 
                if( (str.find(')') != -1) ):
                    close_p_index = str.find(')') 
                    if( (open_p_index<com_index) and (com_index<close_p_index)):
                        return start_index+com_index
                    if ( (open_p_index<com_index) and (close_p_index<com_index)):
                        str = str[com_index+1:]
                        return self.find_comma_in_pths(str,com_index+1)
                        
        return -1
                        
    def normelize(self,ai):
        
        diagnoses = []

        # multiAnswer column?
        if( ('multiAnswer' in ai) and (ai['multiAnswer']) ):
            multi_answer = ai['multiAnswer']

            if( multi_answer == "" or
                multi_answer == "--" or
                multi_answer == "---" or
                multi_answer == "-" 
            ):
                return ai
                
            try:
                ma_json = json.loads(multi_answer)
            except:
                #print("MultiAnswerNormelizer Error ai:",ai)
                return ai

            if("diagnoses" in ma_json and ma_json["diagnoses"]):
                diagnoses = ma_json["diagnoses"]
                
                digs_str = """None of the above
Depression
Anxiety or panic attacks
General Anxiety Disorder (GAD)
Alcoholism
אף אחת מהאפשרויות
no dignosis
PTSD
ADD/ADHD
OCD
ADD (הפרעת קשב וריכוז)
ADHD (הפרעת קשב, ריכוז והיפראקטיביות)
Bipolar disorder
דיכאון
חרדה
סכיזופרניה
PTSD (הפרעת דחק פוסט-טראומטית)
חרדה או התקפי פאניקה
OCD (הפרעה טורדנית כפייתית)
אלכוהוליזם
הפרעות אכילה(אנורקסיה ו/או בולימיה)
הפרעה דו קוטבית
Eating Disorder (Anorexia and/or Bulimia)
ADHD/ADD
Schizophrenia
הפרעת חרדה כללית (GAD)
Anxiety
schits
adhd"""
                
                digs_value = [
                    E_Diagnosis.none,
                    E_Diagnosis.depression,
                    E_Diagnosis.anxiety_panic_attacks,
                    E_Diagnosis.gad,
                    E_Diagnosis.alcoholism,
                    E_Diagnosis.none,
                    E_Diagnosis.none,
                    E_Diagnosis.ptsd,
                    E_Diagnosis.add,
                    E_Diagnosis.ocd,
                    E_Diagnosis.add,
                    E_Diagnosis.adhd,
                    E_Diagnosis.bipolar_disorder,
                    E_Diagnosis.depression,
                    E_Diagnosis.anxiety_panic_attacks,
                    E_Diagnosis.schizophrenia,
                    E_Diagnosis.ptsd,
                    E_Diagnosis.anxiety_panic_attacks,
                    E_Diagnosis.ocd,
                    E_Diagnosis.alcoholism,
                    E_Diagnosis.eating_disorders,
                    E_Diagnosis.bipolar_disorder,
                    E_Diagnosis.eating_disorders,
                    E_Diagnosis.adhd,
                    E_Diagnosis.schizophrenia,
                    E_Diagnosis.gad,
                    E_Diagnosis.anxiety_panic_attacks,
                    E_Diagnosis.schizophrenia,
                    E_Diagnosis.adhd
                     ]
                
                digs_ar = digs_str.split('\n')
                #print(digs_ar)
                digs_dict = {}

                for index,dig_str in  enumerate( digs_ar):
                    digs_dict[dig_str] = digs_value[index]

                for dig in digs_dict:
                    
                    normelized_dig = digs_dict[dig]

                    # go over all diagnoses in array :
                    converted_digs = []
                    for digno in diagnoses: 
                        digno = digno.replace(dig, normelized_dig.name)
                        converted_digs.append(digno)   
                    
                    diagnoses = converted_digs

        # diagnoses column ?
        if( ('diagnoses' in ai) and (ai['diagnoses']) ):
            diagnoses_str = ai['diagnoses']

            if( diagnoses_str == "" or
                diagnoses_str == "--" or
                diagnoses_str == "---" or
                diagnoses_str == "-" 
            ):
                return ai

            # remove ',' within ()
            """
            while (self.find_comma_in_pths(diagnoses_str) != -1):
                # remove comma:
                comma_index = self.find_comma_in_pths(diagnoses_str)
                if len(diagnoses_str) > comma_index:
                    diagnoses_str = diagnoses_str[0 : comma_index : ] + diagnoses_str[comma_index + 1 : :]
            
            """

            digs_value = [
                E_Diagnosis.none,
                E_Diagnosis.ptsd,
                E_Diagnosis.bipolar_disorder,
                E_Diagnosis.none,
                E_Diagnosis.anxiety_panic_attacks,
                E_Diagnosis.add,
                E_Diagnosis.schizophrenia,
                E_Diagnosis.depression,
                E_Diagnosis.anxiety_panic_attacks,
                E_Diagnosis.adhd,
                E_Diagnosis.gad,
                E_Diagnosis.eating_disorders,
                E_Diagnosis.adhd,
                E_Diagnosis.ocd,
                E_Diagnosis.depression,
                E_Diagnosis.eating_disorders,
                E_Diagnosis.gad,
                E_Diagnosis.ocd,
                E_Diagnosis.ptsd,
                E_Diagnosis.alcoholism,
                E_Diagnosis.alcoholism,
                E_Diagnosis.bipolar_disorder, 
                E_Diagnosis.gad,
                E_Diagnosis.eating_disorders, 
                E_Diagnosis.schizophrenia, 
                E_Diagnosis.adhd   
            ]

            digs_str = """אף אחת מהאפשרויות
PTSD (הפרעת דחק פוסט-טראומטית)
הפרעה דו קוטבית
None of the above
חרדה או התקפי פאניקה
ADD (הפרעת קשב וריכוז)
סכיזופרניה
Depression
Anxiety or Panic attacks
ADHD (הפרעת קשב, ריכוז והיפראקטיביות)
Generalized Anxiety Disorder (GAD)
Eating Disorder (anorexia or bulimia)
ADHD (attention deficit hyperactivity disorder)
OCD (הפרעה טורדנית כפייתית)
דיכאון
הפרעות אכילה(אנורקסיה ו/או בולימיה)
הפרעת חרדה כללית (GAD)
OCD (obsessive compulsive disorder)
PTSD (Post Traumatic Stress Disorder)
Alcoholism
אלכוהוליזם
Bipolar Disorder
General Anxiety Disorder (GAD)
Eating disorders (Anorexia/Bulimia)
Schizophrenia
ADHD (Attention Deficit Hyperactivity Disorder)"""

            digs_ar = digs_str.split('\n')
            #print(digs_ar)
            digs_dict = {}

            for index,dig_str in  enumerate( digs_ar):
                digs_dict[dig_str] = digs_value[index]


            for dig in digs_dict:
                normelized_dig = digs_dict[dig]
                diagnoses_str = diagnoses_str.replace(dig, normelized_dig.name)
                
            diagnoses = diagnoses_str.split(",")


        ai["diagnoses"] = diagnoses
        return ai






class NormelizeMeds(NormelizeAIBase):

    def normelize(self,ai):
        
        # no need to noremelize ? (meds column exists already)
        if( 'meds' in ai ):
            return ai

        # multiAnswer column?
        if( ('multiAnswer' in ai) and (ai['multiAnswer']) ):
            multi_answer = ai['multiAnswer']

            if( multi_answer == "" or
                multi_answer == "--" or
                multi_answer == "---" or
                multi_answer == "-" 
            ):
                return ai
                
            try:
                ma_json = json.loads(multi_answer)
            except:
                #print("MultiAnswerNormelizer Error ai:",ai)
                return ai

            if("meds" in ma_json and 
                ma_json["meds"] and
                ma_json["meds"] != '' and 
                ma_json["meds"] != '-' and
                ma_json["meds"] != '--' and
                ma_json["meds"] != '---'  ):
                meds_str = ma_json["meds"]

                meds_dict = {
                    'yes':True,
                    'כן':True,
                    'no':False,
                    'לא':False
                }
                # TBD: normelize meds add hebrew
                meds = meds_dict[meds_str]
                ai["meds"] = meds

        return ai


class NormelizeCreateDiagnosesDict:
    
    def find_comma_in_pths(self,str,start_index=0):
        # "(" ?
        if( (str.find('(') != -1) ):
            open_p_index = str.find('(') 
            
            # "," ?
            if( (str.find(',') != -1) ):
                com_index = str.find(',') 
                if( (str.find(')') != -1) ):
                    close_p_index = str.find(')') 
                    if( (open_p_index<com_index) and (com_index<close_p_index)):
                        return start_index+com_index
                    if ( (open_p_index<com_index) and (close_p_index<com_index)):
                        str = str[com_index+1:]
                        return self.find_comma_in_pths(str,com_index+1)
                        
        return -1
                        
    def update_dict(self,ai,digs_dict = {}):
        
        diagnoses = []

        # diagnoses column ?
        if( ('diagnoses' in ai) and (ai['diagnoses']) ):
            diagnoses_str = ai['diagnoses']

            if( diagnoses_str == "" or
                diagnoses_str == "--" or
                diagnoses_str == "-" 
            ):
                return digs_dict

            # exchage ',' to '$' within ()
            while (self.find_comma_in_pths(diagnoses_str,0) != -1):
                # remove comma:
                comma_index = self.find_comma_in_pths(diagnoses_str,0)
                if len(diagnoses_str) > comma_index:
                    diagnoses_str = diagnoses_str[0 : comma_index : ] + '$' +diagnoses_str[comma_index + 1 : :]
            
            
            diagnoses = diagnoses_str.split(",")

            for dig in diagnoses:
                dig = dig.replace('$',',')
                #print('adding:'+dig)
                digs_dict[dig] = True

            

        return digs_dict




class NormelizeCreateDiagnosesDictA:
    
    def find_comma_in_pths(self,str,start_index=0):
        # "(" ?
        if( (str.find('(') != -1) ):
            open_p_index = str.find('(') 
            
            # "," ?
            if( (str.find(',') != -1) ):
                com_index = str.find(',') 
                if( (str.find(')') != -1) ):
                    close_p_index = str.find(')') 
                    if( (open_p_index<com_index) and (com_index<close_p_index)):
                        return start_index+com_index
                    if ( (open_p_index<com_index) and (close_p_index<com_index)):
                        str = str[com_index+1:]
                        return self.find_comma_in_pths(str,com_index+1)
                        
        return -1
                        
    def update_dict(self,ai,digs_dict = {}):
        
        diagnoses = []

        if( ('multiAnswer' in ai) and (ai['multiAnswer']) ):
            multi_answer = ai['multiAnswer']

            if( multi_answer == "" or
                multi_answer == "--" or
                multi_answer == "--" or 
                multi_answer == "-"
            ):
                return digs_dict
                
            try:
                ma_json = json.loads(multi_answer)
            except:
                #print("MultiAnswerNormelizer Error ai:",ai)
                return digs_dict

            if("diagnoses" in ma_json and ma_json["diagnoses"]):
                diagnoses = ma_json["diagnoses"]
                

            for dig in diagnoses:
          
                #print('adding:'+dig)
                digs_dict[dig] = True

            

        return digs_dict


