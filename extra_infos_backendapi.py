# Description: This script check each trials and fetch the latest "estimated" enrollment count
import pymysql
import pandas as pd
from lxml import html
import time
import pandas as pd
import json
import requests
import mariadb
database = "ct_final"


conn = mariadb.connect(
host = "localhost",
user = "root",
password = "root",
port=3306,
database=database)

c = conn.cursor()
# SQL query for creating table if it doesn't exist
c.execute("""
CREATE TABLE IF NOT EXISTS clinicaltrial_extra (
nct_id VARCHAR(50) PRIMARY KEY,
is_Rare BOOLEAN,
overallStatus VARCHAR(255),
startYear YEAR,
last_enrollment_count INT,
original_enrollment_count INT,
enrolled_less_than_original BOOLEAN,
enrollmentCountSame BOOLEAN,
conditions_count INT,
masking_count VARCHAR(255),
Participant_masked BOOLEAN,
Care_provider_masked BOOLEAN,
Investigator_masked BOOLEAN,
Outcomes_assessor_masked BOOLEAN,
condition_mesh_count INT,
intervention_mesh_count INT,
primaryoutcome_count INT,
secondaryoutcome_count INT,
original_primaryoutcome_count INT,
original_secondaryoutcome_count INT,
topic_drugInfo TEXT,
topic_rareDiseases TEXT,
topic_medlinePlus TEXT,
topic_medlinePlusGenetics TEXT,
primaryOutcomesSame BOOLEAN,
secondaryOutcomesSame BOOLEAN,
orgFullNameSame BOOLEAN,
responsiblePartySame BOOLEAN,
leadSponsorSame BOOLEAN,
versions_count INT

) CHARACTER SET utf8mb4;
""")

def insert_topic_drugInfo(nct_id, topic_drugInfo,c):
    c.execute("""CREATE TABLE IF NOT EXISTS topic_drugInfo (
    nct_id VARCHAR(50),
    drugInfo VARCHAR(100),
    drugbank_id VARCHAR(100),
    PRIMARY KEY (nct_id, drugInfo))""")
    # insert the pair in the table if it doesn't exist
    for drugInfo in topic_drugInfo:
        c.execute("INSERT INTO topic_drugInfo (nct_id, drugInfo) VALUES (%s, %s) ON DUPLICATE KEY UPDATE nct_id = VALUES(nct_id), drugInfo = VALUES(drugInfo)", (nct_id, drugInfo))
        conn.commit()






def fetch_and_update(c):
    '''c.execute("""SELECT DISTINCT nct_id FROM clinicaltrial WHERE  
                    phases NOT IN ("['NA']","['PHASE4']","['EARLY_PHASE1']")
                   and study_type ="INTERVENTIONAL" 
                   AND overallStatus IN ('TERMINATED','COMPLETED','WITHDRAWN')
                    AND nct_id NOT IN (SELECT DISTINCT nct_id FROM clinicaltrial_extra);""")'''
    c.execute("""
        SELECT DISTINCT nct_id 
        FROM clinicaltrial 
        WHERE nct_id NOT IN (SELECT DISTINCT nct_id FROM clinicaltrial_extra)
        ORDER BY RAND()
        LIMIT 200;
    """)
    rows = c.fetchall()
    #time.sleep(1)
    i=0
    total = len(rows)
    if len(rows) == 0:
        print("No rows to process")
        return
    for row in rows:
        i+=1
        nct_id = row[0] if row else None
        print(f"Processing {i}/{total} row {row[0]} for {nct_id}")
        # Fetch the value of nct_id
        
        val = f'https://clinicaltrials.gov/api/int/studies/{nct_id}?query.id={nct_id}&history=true'
        response = requests.get(val)

        if response.status_code == 200:
            # Parse the response as JSON
            data = response.json()
            # fetch versions with updated enrol
            if 'study' in data:
                data_study = data['study']
                overallstatus = data_study['protocolSection']['statusModule']['overallStatus']
                date_string = data_study['protocolSection']['statusModule']['startDateStruct']['date']
                startYear = int(date_string[:4])
                
                conditions = data_study['protocolSection'].get('conditionsModule', {}).get('conditions', [])
                conditions_count = len(conditions)
                try:
                    masking_info = data_study['protocolSection']['designModule']['designInfo']['maskingInfo']
                    masking_count = masking_info['masking']

                    # Get whoMasked list, or empty list if it doesn't exist
                    who_masked = masking_info.get('whoMasked', [])

                    # You can now check who is masked, for example:
                    Participant_masked = "PARTICIPANT" in who_masked
                    Care_provider_masked = "CARE_PROVIDER" in who_masked
                    Investigator_masked = "INVESTIGATOR" in who_masked
                    Outcomes_assessor_masked = "OUTCOMES_ASSESSOR" in who_masked
                except:
                    masking_count = 0
                    Participant_masked = False
                    Care_provider_masked = False
                    Investigator_masked = False
                    Outcomes_assessor_masked = False
                
                last_enrollment_count = data_study['protocolSection']['designModule']['enrollmentInfo']['count']
                
                if "outcomesModule" in data_study['protocolSection']:
                    outcomes_module = data_study['protocolSection']["outcomesModule"]
                    primaryoutcome_count = len(outcomes_module.get("primaryOutcomes", []))
                    secondaryoutcome_count = len(outcomes_module.get("secondaryOutcomes", []))
                
                #print(f'ncit_id: {nct_id}, overallstatus: {overallstatus}, startYear: {startYear}, last_enrollment_count: {last_enrollment_count}, conditions_count: {conditions_count}, masking_count: {masking_count}, Participant_masked: {Participant_masked}, Care_provider_masked: {Care_provider_masked}, Investigator_masked: {Investigator_masked}, Outcomes_assessor_masked: {Outcomes_assessor_masked}, primaryoutcome_count: {primaryoutcome_count}, secondaryoutcome_count: {secondaryoutcome_count}')
                
                condition_mesh = data_study['derivedSection'].get('conditionBrowseModule', {}).get('meshes', [])
                condition_mesh_count = len(condition_mesh)
                intervention_mesh = data_study['derivedSection'].get('interventionBrowseModule', {}).get('meshes', [])
                intervention_mesh_count = len(intervention_mesh)
                #print(f'condition_mesh_count: {condition_mesh_count}, intervention_mesh_count: {intervention_mesh_count}')
            
            
            if "topics" in data:
                topic_medlinePlusGenetics =  [item['name'] for item in data.get('topics', {}).get('medlinePlusGenetics', [])]
                topic_medlinePlus =  [item['name'] for item in data.get('topics', {}).get('medlinePlus', [])]
                topic_rareDiseases =  [item['name'] for item in data.get('topics', {}).get('rareDiseases', [])]
                topic_drugInfo =  [item['name'] for item in data.get('topics', {}).get('drugInfo', [])]
                if topic_drugInfo:
                    insert_topic_drugInfo(nct_id, topic_drugInfo,c)
                    
                is_Rare = bool(topic_rareDiseases)
                #print(f'topic_medlinePlusGenetics: {topic_medlinePlusGenetics}, topic_medlinePlus: {topic_medlinePlus}, topic_rareDiseases: {topic_rareDiseases},is_Rare:{is_Rare}, topic_drugInfo: {topic_drugInfo}')
            else:
                topic_medlinePlusGenetics = []
                topic_medlinePlus = []
                topic_rareDiseases = []
                topic_drugInfo = []
                is_Rare = False
                
            if "history" in data:
                history = data['history']
                changes = history['changes']
                versions_count = len(changes)
                #print(f'versions_count: {versions_count}')
                
                if "originalData" in history:
                    original_data = history['originalData']
                    original_primaryoutcome_count = int(len(original_data.get("primaryOutcomes", [])))
                    original_secondaryoutcome_count = int(len(original_data.get("secondaryOutcomes", [])))
                    if primaryoutcome_count == original_primaryoutcome_count:
                        primaryOutcomesSame = True
                    else:
                        primaryOutcomesSame = False
                    if secondaryoutcome_count == original_secondaryoutcome_count:
                        secondaryOutcomesSame = True
                    else:
                        secondaryOutcomesSame = False
                    
                    #print(f'original_primaryoutcome_count: {original_primaryoutcome_count}, original_secondaryoutcome_count: {original_secondaryoutcome_count}')
                    
                    
                    original_enrollment_count = original_data.get('enrollmentInfo', {}).get('count', 0)
                    #print(f'original_enrollment_count: {original_enrollment_count}, last_enrollment_count: {last_enrollment_count}')
                    if last_enrollment_count < original_enrollment_count:
                        enrolled_less_than_original = True
                        enrollmentCountSame = False
                    elif last_enrollment_count == original_enrollment_count:
                        enrollmentCountSame = True
                        enrolled_less_than_original = False
                    else:
                        enrolled_less_than_original = False
                        enrollmentCountSame = False
                        
                    
                    orgFullNameSame = bool("orgFullNameSame" in original_data)
                    responsiblePartySame = bool("responsiblePartySame" in original_data)        
                    leadSponsorSame = bool("leadSponsorSame" in original_data)
                    
            print("Summary:")
            print(f'nct_id: {nct_id}, /q overallstatus: {overallstatus}, startYear: {startYear}, last_enrollment_count: {last_enrollment_count}, conditions_count: {conditions_count}, masking_count: {masking_count}, Participant_masked: {Participant_masked}, Care_provider_masked: {Care_provider_masked}, Investigator_masked: {Investigator_masked}, Outcomes_assessor_masked: {Outcomes_assessor_masked}, primaryoutcome_count: {primaryoutcome_count}, secondaryoutcome_count: {secondaryoutcome_count}, condition_mesh_count: {condition_mesh_count}, intervention_mesh_count: {intervention_mesh_count}, topic_medlinePlusGenetics: {topic_medlinePlusGenetics}, topic_medlinePlus: {topic_medlinePlus}, topic_rareDiseases: {topic_rareDiseases},is_Rare:{is_Rare}, topic_drugInfo: {topic_drugInfo}, versions_count: {versions_count}, original_primaryoutcome_count: {original_primaryoutcome_count}, original_secondaryoutcome_count: {original_secondaryoutcome_count}, original_enrollment_count: {original_enrollment_count}, orgFullNameSame: {orgFullNameSame}, responsiblePartySame: {responsiblePartySame}, leadSponsorSame: {leadSponsorSame}')
            c.execute("""INSERT INTO clinicaltrial_extra (nct_id, is_Rare, overallStatus, startYear, last_enrollment_count,
                      original_enrollment_count, enrolled_less_than_original, enrollmentCountSame, conditions_count,
                      masking_count, Participant_masked, Care_provider_masked, Investigator_masked, Outcomes_assessor_masked,
                      condition_mesh_count, intervention_mesh_count, primaryoutcome_count, secondaryoutcome_count, topic_drugInfo,
                      topic_rareDiseases, topic_medlinePlus, topic_medlinePlusGenetics, original_primaryoutcome_count,
                      original_secondaryoutcome_count, primaryOutcomesSame, secondaryOutcomesSame, orgFullNameSame, responsiblePartySame,
                      leadSponsorSame, versions_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                      %s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE nct_id = VALUES(nct_id), is_Rare = VALUES(is_Rare), overallStatus = VALUES(overallStatus), startYear = VALUES(startYear),
                      last_enrollment_count = VALUES(last_enrollment_count), original_enrollment_count = VALUES(original_enrollment_count),
                      enrolled_less_than_original = VALUES(enrolled_less_than_original), enrollmentCountSame = VALUES(enrollmentCountSame),
                      conditions_count = VALUES(conditions_count), masking_count = VALUES(masking_count), Participant_masked = VALUES(Participant_masked),
                      Care_provider_masked = VALUES(Care_provider_masked), Investigator_masked = VALUES(Investigator_masked),
                      Outcomes_assessor_masked = VALUES(Outcomes_assessor_masked), condition_mesh_count = VALUES(condition_mesh_count),
                      intervention_mesh_count = VALUES(intervention_mesh_count), primaryoutcome_count = VALUES(primaryoutcome_count),
                      secondaryoutcome_count = VALUES(secondaryoutcome_count), topic_drugInfo = VALUES(topic_drugInfo), 
                      topic_rareDiseases = VALUES(topic_rareDiseases), topic_medlinePlus = VALUES(topic_medlinePlus),
                      topic_medlinePlusGenetics = VALUES(topic_medlinePlusGenetics), original_primaryoutcome_count = VALUES(original_primaryoutcome_count), 
                      original_secondaryoutcome_count = VALUES(original_secondaryoutcome_count), primaryOutcomesSame = VALUES(primaryOutcomesSame), 
                      secondaryOutcomesSame = VALUES(secondaryOutcomesSame), orgFullNameSame = VALUES(orgFullNameSame),
                      responsiblePartySame = VALUES(responsiblePartySame), leadSponsorSame = VALUES(leadSponsorSame),
                      versions_count = VALUES(versions_count)""",
                      (nct_id, is_Rare, overallstatus, startYear, last_enrollment_count,
                       original_enrollment_count, enrolled_less_than_original, enrollmentCountSame, conditions_count,
                       masking_count, Participant_masked, Care_provider_masked, Investigator_masked, Outcomes_assessor_masked, 
                       condition_mesh_count, intervention_mesh_count, primaryoutcome_count, secondaryoutcome_count,
                       json.dumps(topic_drugInfo), json.dumps(topic_rareDiseases), json.dumps(topic_medlinePlus), json.dumps(topic_medlinePlusGenetics),
                       original_primaryoutcome_count, original_secondaryoutcome_count, primaryOutcomesSame, secondaryOutcomesSame, orgFullNameSame,
                       responsiblePartySame, leadSponsorSame, versions_count))
            conn.commit()
                
                
                
while True :                
    fetch_and_update(c)             


    