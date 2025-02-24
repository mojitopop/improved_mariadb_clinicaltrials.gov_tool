import os
import json
import glob
import mariadb
import sys
import pandas as pd
import time 
import subprocess
import requests
import argparse
import datetime
from sqlalchemy import create_engine

# Define the database name once for all the connections dict of the script
database = "ct_final"


# Define the default values for all the variables in case they are not provided in a trial
class ClinicalTrial:
    def __init__(self):
        self.nct_id = ""  # TEXT
        self.is_Rare = False
        self.lastUpdateSubmitDate = ""
        self.org_name = ""
        self.org_study_class = ""  # VARCHAR(25)
        self.primaryCompletionDate = ""  # VARCHAR(25)
        ### statsModule
        self.overallStatus = ""
        self.hasExpandedAccess = False
        self.startDate = ""
        self.completionDate = ""
        self.lead_sponsor = ""
        self.sponsor_class = ""
        ### oversightModule
        self.oversighthasdmc = False
        self.isfdaregulateddrug = False
        ### conditionsModule
        self.conditions = []
        self.keywords = []
        ### designModule
        self.study_type = ""
        self.phases = []
        self.interventionModel = ""
        self.timePerspective = ""
        self.primaryPurpose = ""
        self.mask_type = ""
        self.theorical_enrollment_count = 0
        self.last_enrollment_count = 0
        self.arm_group_count = 1
        ### armInterventionModule
        self.intervention_type = ""
        self.interventions = {}
        self.healthyVolunteers = False
        self.sex = ""
        self.minimumAge = ""
        self.maximumAge = ""
        self.AgeMetric = ""
        self.stdAges = []
        self.why_stopped = ""  # TEXT
        self.has_results = False  # BOOLEAN
        self.randomized = ""
        self.nb_secondaryOutcomes = 0
        self.nb_primaryOutcomes = 0
        self.facility = ""
        self.name = ""
        self.role = ""
        self.eligibility = ""
        self.responsibleParty = ""
        self.total_centers = 1
        self.country = ""
        self.subregion = ""
        self.continent = ""

# Use regex to explicit the "OTHER" class using the name of the entity into University, Hospital or Network
def get_updated_class(clinical_trial,old_class):
    updated_class = old_class
    
    university = ["universi", "university", "college", "school", "academ", "center", "faculty", "campus", "polytechnic", "polytechnique"]
    hospital = ['hospit', 'medic', 'clinic', 'hôpit', 'hopit', 'krankenhaus', 'klinik','Ospedale']
    networks = ['group', 'network', 'society']
    
    for uni in university:
        if uni.lower() in clinical_trial.lower():  # .lower() to make it case-insensitive
            updated_class = "UNIVERSITY"
            break
    for hop in hospital:
        if hop.lower() in clinical_trial.lower():
            updated_class = "HOSPITAL"
            break
    for net in networks:
        if net.lower() in clinical_trial.lower():
            updated_class = "NETWORK"
            break
    
    return updated_class

# Format the date string to have the format "YYYY-MM-DD" as some trials only have the year and month
def format_date(date_str):
    # If the date string contain less than 7 characters, append "-01" to it
    if len(date_str) < 10 and len(date_str) > 0:
        date_str += "-01"
    if len(date_str) == 0:
        date_str = "1900-01-01"
    return date_str

# insert in a different table all the different collaborators per trials as some as more than one
def insert_collaborators(collaborators, nct_id, conn):
    cursor = conn.cursor()

    # SQL query for creating table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS collaborators (
        name VARCHAR(255),
        class VARCHAR(255),
        nct_id VARCHAR(255),
        number_of_collaborators INT,
        PRIMARY KEY (name, nct_id)
    )
    """
    cursor.execute(create_table_query)

    insert_or_replace_collaborator = """
    INSERT INTO Collaborators (name, class, nct_id)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE name=VALUES(name), class=VALUES(class), nct_id=VALUES(nct_id)
    """



                

    for collab in collaborators:
        
        collab['class']=get_updated_class(collab['name'],collab['class'])
        data_collaborator = (collab['name'], collab['class'], nct_id)
        cursor.execute(insert_or_replace_collaborator, data_collaborator)

    conn.commit()
    cursor.close()

# insert in a different table all the mesh terms per trials, categorized by tree level and condition/intervention mesh type and remove the common meshs (ie All Drugs and Chemicals, All Conditions, Symptoms and General Pathology)
def insert_mesh(locations, nct_id, conn,dict_name):
    cursor = conn.cursor()

    # SQL query for creating table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS termes_mesh (
        term TEXT,
        mesh_id VARCHAR(255),
        mesh_id_formatted VARCHAR(255),
        tree_level varchar(255),
        mesh_category VARCHAR(255),
        nct_id VARCHAR(255),
        PRIMARY KEY (mesh_id, nct_id)
    )
    """
    cursor.execute(create_table_query)

    # SQL query for inserting a location or updating if it already exists
    insert_or_replace_location = """
    INSERT INTO termes_mesh (term,mesh_id,mesh_id_formatted,tree_level,mesh_category, nct_id)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE term=VALUES(term), mesh_id=VALUES(mesh_id), mesh_id_formatted=VALUES(mesh_id_formatted), 
    tree_level=VALUES(tree_level), mesh_category=VALUES(mesh_category), nct_id=VALUES(nct_id)
    """
    if dict_name == "condition_mesh":
        mesh_category = "condition"
        tree_level = "main"
    elif dict_name == "intervention_mesh":
        mesh_category = "intervention"
        tree_level = "main"
    elif dict_name == "condition_ancestors":
        mesh_category = "condition"
        tree_level = "ancestors"
    elif dict_name == "intervention_ancestors":
        mesh_category = "intervention"
        tree_level = "ancestors"
    elif dict_name == "condition_browseBranches":
        mesh_category = "condition"
        tree_level = "browseBranches"
    elif dict_name == "intervention_browseBranches":
        mesh_category = "intervention"
        tree_level = "browseBranches"


    
    for location in locations:
        
        if dict_name == "condition_browseBranches" or dict_name == "intervention_browseBranches":
            location['id'] = location['abbrev']
            location['term'] = location['name']
            mesh_id_formatted = None
        
        else:
            if len(location['id']) == 10 and location['id'][1:4] == '000':
                mesh_id_formatted = location['id'][0] + location['id'][4:]
            else:
                mesh_id_formatted = location['id']
        
        if location['term'] in ["All Drugs and Chemicals", "All Conditions",'Symptoms and General Pathology','Amino Acids']:
            continue

        data_location = (location['term'], location['id'], mesh_id_formatted, tree_level,mesh_category, nct_id)
        cursor.execute(insert_or_replace_location, data_location)

    # commit the changes
    conn.commit()
    cursor.close()


# insert in a different table all the secondary ids per trials, categorized by type and domain to try to cross reference trials with database others than clinicaltrials.gov
def insert_secondary_id(secondary_id,nct_id, conn):
    cursor = conn.cursor()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS secondary_ids (
        nct_id VARCHAR(12),
        sec_id VARCHAR(35),
        type VARCHAR(35),
        domain VARCHAR(120),
        PRIMARY KEY (nct_id, sec_id))"""
    cursor.execute(create_table_query)
    insert_or_replace_secondary_id = """
    INSERT INTO secondary_ids (nct_id, sec_id, type, domain)VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE nct_id=VALUES(nct_id), sec_id=VALUES(sec_id), type=VALUES(type), domain=VALUES(domain)
    """
    sec_id = secondary_id['id'] if 'id' in secondary_id else "UNKNOWN"
    id_type = secondary_id['type'] if 'type' in secondary_id else "UNKNOWN"
    domain = secondary_id['domain'] if 'domain' in secondary_id else "UNKNOWN"

    data_location = (nct_id,sec_id,id_type,domain)
    cursor.execute(insert_or_replace_secondary_id, data_location)

    conn.commit()
    cursor.close()

# insert in a different table all the references per trials (need sanitization)
def insert_eligibility_criteria(clinical_trial, conn):
    c = conn.cursor()
        
    c.execute('''
        CREATE TABLE IF NOT EXISTS eligibility (
            nct_id VARCHAR(50) PRIMARY KEY,
            overallStatus VARCHAR(25),
            study_type VARCHAR(25),
            startDate DATE,
            eligibility TEXT
        ) CHARACTER SET utf8mb4;
    ''')
    c.execute('''INSERT INTO eligibility
            (nct_id, overallStatus, study_type, startDate,eligibility)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            overallStatus = VALUES(overallStatus),
            study_type = VALUES(study_type),
            startDate = VALUES(startDate),
            eligibility = VALUES(eligibility)
        ''', (clinical_trial.nct_id, clinical_trial.overallStatus, clinical_trial.study_type, clinical_trial.startDate,clinical_trial.eligibility))

# for each trial use the record history backend API to get the theorical enrollment count stated in the very first version of the trial
def get_theorical_enrollment_count(nct_id):
    theorical_enrollment_count = 0
    url = f'https://clinicaltrials.gov/api/int/studies/{nct_id}/history/0'
    #time.sleep(0.01)
    response =  requests.get(url)
    if response.status_code == 200:
        # Parse the response as JSON
        data =  response.json()
        try:
            theorical_enrollment_count=data['study']['protocolSection']['designModule']['enrollmentInfo'].get("count",0)
        except: theorical_enrollment_count = 0
    return theorical_enrollment_count

# insert locations of every centers for each trials along with geolocation data    
def insert_locations(locations, nct_id,startDate, conn):
    cursor = conn.cursor()

    create_table_query = """
        CREATE TABLE IF NOT EXISTS Locations (
            facility VARCHAR(255),
            startDate DATE,
            city VARCHAR(100),
            status VARCHAR(255),
            state VARCHAR(255),
            country VARCHAR(60),
            continent varchar(255),
            lat FLOAT,
            lon FLOAT,
            nct_id VARCHAR(12),
            total_center INT,
            location_number INT,
            PRIMARY KEY (nct_id, facility, total_center, location_number)
        )
        """
    cursor.execute(create_table_query)

    # SQL query for inserting a location or updating if it already exists
    insert_or_replace_location = """
        INSERT INTO Locations (facility,startDate, city, status, state, country, lat, lon, nct_id, total_center, location_number)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
        ON DUPLICATE KEY UPDATE status=VALUES(status)
        """

    # insert each location
    total_center = len(locations)
    for index, location in enumerate(locations):
        lat = location['geoPoint']['lat'] if 'geoPoint' in location and 'lat' in location['geoPoint'] else None
        lon = location['geoPoint']['lon'] if 'geoPoint' in location and 'lon' in location['geoPoint'] else None
        status = location['status'] if 'status' in location else None
        facility = location['facility'] if 'facility' in location else "unknown"
        city = location['city'] if 'city' in location else None
        country = location['country'] if 'country' in location else None
        state = location['state'] if 'state' in location else None
        location_number = index + 1  # Assign a unique number to each location
        data_location = (facility,startDate, city, status, state, country, lat, lon, nct_id, total_center, location_number)
    
        cursor.execute(insert_or_replace_location, data_location)

    conn.commit()
    cursor.close()

# insert location of the responsible party for each trials along with geolocation data
# this will be defined as main location for the trial
# if the responsible party is the sponsor, the location will be the lead sponsor location but might be too vague and need additional post processing
# if the responsible part is sponsor, all the informations related to the PI are set to 0 
def insert_responsibleparty_location(clinical_trial,responsiblePartyModule,conn):
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS responsibleparty_location(
        nct_id VARCHAR(12),
        responsible_party VARCHAR(25),
        startDate DATE,
        lead_sponsor TEXT,
        sponsor_class VARCHAR(25),
        PI_full_name TEXT,
        PI_title TEXT,
        PI_affiliation TEXT,
        city VARCHAR(100),
        country VARCHAR(60),
        continent varchar(255),
        lat FLOAT,
        lon FLOAT,
        PRIMARY KEY (nct_id))"""
    cursor.execute(create_table_query)
    
    # SQL query for inserting a location or updating if it already exists
    insert_or_replace_location = """
        INSERT IGNORE INTO responsibleparty_location (nct_id, responsible_party,startDate,lead_sponsor,sponsor_class, PI_full_name,PI_title,
        PI_affiliation) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """
    if clinical_trial.responsibleParty == 'SPONSOR':
        PI_full_name = "0"
        PI_title = "0"
        PI_affiliation = "0"
    else :
        PI_full_name = responsiblePartyModule.get('investigatorFullName',"")
        PI_title = responsiblePartyModule.get('investigatorTitle',"")
        PI_affiliation = responsiblePartyModule.get('investigatorAffiliation',"")
        
        
    cursor.execute(insert_or_replace_location, (clinical_trial.nct_id, clinical_trial.responsibleParty,
                                                clinical_trial.startDate, clinical_trial.lead_sponsor, clinical_trial.sponsor_class,
                                                PI_full_name,PI_title,PI_affiliation))
    conn.commit()
    cursor.close()
    

# same as the other locations funct but unused for now
def insert_overall_official_locations(clinical_trial, conn):
    cursor = conn.cursor()

    create_table_query = """
        CREATE TABLE IF NOT EXISTS overallofficial_location (
            nct_id VARCHAR(12),
            facility VARCHAR(255),
            startDate DATE,
            name varchar(255),
            role varchar(100),
            city VARCHAR(100),
            country VARCHAR(60),
            continent varchar(255),
            lat FLOAT,
            lon FLOAT,
            PRIMARY KEY (nct_id, facility, name,role)
        )
        """
    cursor.execute(create_table_query)

    # SQL query for inserting a location or updating if it already exists
    insert_or_replace_location = """
        INSERT IGNORE INTO overallofficial_location (nct_id, facility,startDate, name, role) VALUES (%s, %s,%s, %s, %s)
    """
    cursor.execute(insert_or_replace_location, (clinical_trial.nct_id, clinical_trial.facility,clinical_trial.startDate, clinical_trial.name, clinical_trial.role))
    conn.commit()
    cursor.close()


# create a table with all the different arm groups ID and the number of dropouts per group+dropouts reasons
def insert_dropout(dropout_df, conn):
    cursor = conn.cursor()

    # SQL query for creating table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS withdraws_type (
        groupId VARCHAR(255),
        nct_id VARCHAR(255),
        numSubjects INT,
        dropWithdraws_type VARCHAR(255),
        overallStatus VARCHAR(255),
        why_stopped TEXT,
        PRIMARY KEY (groupId, nct_id)
    )
    """
    cursor.execute(create_table_query)

    insert_or_replace_dropout = """
    INSERT INTO withdraws_type (groupId, nct_id, numSubjects, dropWithdraws_type, overallStatus, why_stopped)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE groupId=VALUES(groupId), nct_id=VALUES(nct_id), numSubjects=VALUES(numSubjects),
    dropWithdraws_type=VALUES(dropWithdraws_type), overallStatus=VALUES(overallStatus), why_stopped=VALUES(why_stopped)
    """

    

    # Filter out rows where 'numSubjects' is 0
    dropout_df['numSubjects'] = pd.to_numeric(dropout_df['numSubjects'], errors='coerce').fillna(0).astype(int)

    filtered_df = dropout_df[dropout_df['numSubjects'] != "0"]

    # Iterate over the remaining rows
    for index, dropout in filtered_df.iterrows():
        data_dropout = (dropout['groupId'], dropout['nct_id'], dropout['numSubjects'], dropout['parent_dropWithdraws.type'],
                         dropout['overallStatus'], dropout['why_stopped'])
        cursor.execute(insert_or_replace_dropout, data_dropout)

    conn.commit()
    cursor.close()


# create a table with all the PMIDS references for each trials and where it was cited (ie results, derived, etc)
def insert_references(reference,nct_id,conn):
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS pmid_references(
        nct_id VARCHAR(15),
        pmid INT,
        type VARCHAR(35),
        citation TEXT,
        PRIMARY KEY (nct_id, pmid))"""
    cursor.execute(create_table_query)

    type = reference['type'] if 'type' in reference else None
    citation = reference['citation'] if 'citation' in reference else None
    pmid= reference['pmid'] if 'pmid' in reference else 0

    insert_or_replace_reference = """
    INSERT INTO pmid_references (nct_id, pmid, type, citation) VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE nct_id=VALUES(nct_id), pmid=VALUES(pmid), type=VALUES(type), citation=VALUES(citation)"""
    cursor.execute(insert_or_replace_reference, (nct_id, pmid,type,citation))
    conn.commit()
    cursor.close()

# some data for big milestone and some numbers ie nb of patients and nb of each sex
def insert_baseline_characteristics(merged_df, conn):
    cursor = conn.cursor()

    # SQL query for creating table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS baseline_characteristics (
        groupId VARCHAR(255),
        nct_id VARCHAR(255),
        title TEXT,
        status VARCHAR(255),
        numSubjects INT,
        Female_count INT,
        Male_count INT,
        numSubjects_who_completed INT,
        studyStatus VARCHAR(255),
        PRIMARY KEY (groupId, nct_id)
    )
    """
    cursor.execute(create_table_query)

    insert_or_replace_charact = """
    INSERT INTO baseline_characteristics (groupId, nct_id, title, status, numSubjects, Female_count,Male_count,numSubjects_who_completed,studyStatus)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE groupId=VALUES(groupId), nct_id=VALUES(nct_id), title=VALUES(title),status=VALUES(status), 
    numSubjects=VALUES(numSubjects), Female_count=VALUES(Female_count),Male_count=VALUES(Male_count),
    numSubjects_who_completed=VALUES(numSubjects_who_completed),studyStatus=VALUES(studyStatus)
    """


    ##### Correct for the idiots who put sex in all caps instead of just the first letter
    # Convert column names to lower case for comparison
    lowercased_columns = list(map(str.lower, merged_df.columns))

    # Check if 'male' column exists (case-insensitive)
    if 'male' in lowercased_columns:
        # If exists, rename it to 'Male'
        merged_df.rename(columns={merged_df.columns[list(lowercased_columns).index('male')]: 'Male'}, inplace=True)
    elif 'Male' not in merged_df.columns:
        # If not, create it and fill with 0
        merged_df['Male'] = 0

    # Check if 'female' column exists (case-insensitive)
    if 'female' in lowercased_columns:
        # If exists, rename it to 'Female'
        merged_df.rename(columns={merged_df.columns[list(lowercased_columns).index('female')]: 'Female'}, inplace=True)
    elif 'Female' not in merged_df.columns:
        # If not, create it and fill with 0
        merged_df['Female'] = 0
        print(merged_df)
       

    # Convert to integer
    merged_df['Male'] = pd.to_numeric(merged_df['Male'], errors='coerce').fillna(0).astype(int)
    merged_df['Female'] = pd.to_numeric(merged_df['Female'], errors='coerce').fillna(0).astype(int)
    merged_df['nb_de_patients'] = pd.to_numeric(merged_df['nb_de_patients'], errors='coerce').fillna(0).astype(int)
    merged_df['numSubjects'] = pd.to_numeric(merged_df['numSubjects'], errors='coerce').fillna(0).astype(int)

    for index, merged in merged_df.iterrows():
        data_merged = (merged['groupId'], merged['nct_id'], merged['parent_title'],merged['parent_milestones.type'],
                       merged['numSubjects'], merged['Female'],
                        merged['Male'], merged['nb_de_patients'], merged['overallStatus'])
        cursor.execute(insert_or_replace_charact, data_merged)

    conn.commit()
    cursor.close()


# the actual function that parse the json and extract data (similarly to beautifulsoup for webscrapping)
def parse_json(data,conn):
    clinical_trial = ClinicalTrial()
    

    if "protocolSection" in data:
        protocol_section = data["protocolSection"]

        if "identificationModule" in protocol_section:
            id_module = protocol_section["identificationModule"]
            clinical_trial.nct_id = id_module.get("nctId", "")
            org = id_module.get("organization", {})
            clinical_trial.org_name = org.get("fullName", "")

            org_study_class = org.get("class", "")
            clinical_trial.org_study_class = get_updated_class(clinical_trial.org_name,org_study_class)
            
            ### secondaryIDS infos add data cleaning function
            if 'secondaryIdInfos' in id_module:
                secondary_ids = id_module['secondaryIdInfos']
                if secondary_ids:
                    for secondary_id in secondary_ids:
                        insert_secondary_id(secondary_id, clinical_trial.nct_id, conn)


        if "statusModule" in protocol_section:
            status_module = protocol_section["statusModule"]
            clinical_trial.overallStatus = status_module.get("overallStatus","")
            clinical_trial.hasExpandedAccess = status_module.get("expandedAccessInfo", {}).get("hasExpandedAccess", 0)
            clinical_trial.startDate = format_date(status_module.get("startDateStruct",{}).get("date",""))
            clinical_trial.primaryCompletionDate = format_date(status_module.get("primaryCompletionDateStruct",{}).get("date",""))
            clinical_trial.completionDate = format_date(status_module.get("completionDateStruct",{}).get("date",""))

            clinical_trial.lastUpdateSubmitDate = format_date(status_module.get("lastUpdateSubmitDate",""))
            clinical_trial.why_stopped = status_module.get("whyStopped", None)
            
        ############## check if update is needed for the trial
        c = conn.cursor()
        c.execute("SELECT lastUpdateSubmitDate FROM clinicaltrial WHERE nct_id = ?",(clinical_trial.nct_id,))
        last_update = c.fetchone()
        current_date = datetime.datetime.strptime(clinical_trial.lastUpdateSubmitDate, '%Y-%m-%d').date()
        if last_update is not None:
            last_update_date = last_update[0]
            if last_update_date == current_date:
                print(f"Trial {clinical_trial.nct_id} is up to date")
                return None
        if last_update is not None:
            print(f"Updating trial {clinical_trial.nct_id} from {last_update_date} to {current_date}")
        else:
            print(f"Creating trial {clinical_trial.nct_id}")


        ##### if responsibleparty = sponsor = get address from overallofficial contact
        ##### if it's PI, get it from PI affiliation
        ##### always get lead sponsor name + first name in overallofficial loc
        if "sponsorCollaboratorsModule" in protocol_section:
            sponsor_module = protocol_section["sponsorCollaboratorsModule"]
            clinical_trial.responsibleParty = sponsor_module.get("responsibleParty",{}).get("type","")
            responsiblePartyModule = sponsor_module.get("responsibleParty",{})
            ####### add parsing for collaborator
            clinical_trial.lead_sponsor = sponsor_module.get("leadSponsor",{}).get("name","")
            sponsor_class =sponsor_module.get("leadSponsor",{}).get("class","")
            clinical_trial.sponsor_class = get_updated_class(clinical_trial.lead_sponsor,sponsor_class) 
            
        #### insert responsibleparty location that define the trial's main location
        insert_responsibleparty_location(clinical_trial,responsiblePartyModule,conn)




        if "sponsorCollaboratorsModule" in protocol_section:
            sponsor_module = protocol_section["sponsorCollaboratorsModule"]
            collaborators = sponsor_module.get("collaborators", [])
            nct_id = data["protocolSection"]["identificationModule"].get("nctId")
            insert_collaborators(collaborators, nct_id,conn)   



        if "oversightModule" in protocol_section:
            clinical_trial.oversighthasdmc = protocol_section["oversightModule"].get("oversightHasDmc",False)
            clinical_trial.isfdaregulateddrug = protocol_section["oversightModule"].get("isFdaRegulatedDrug",False)

        

        #### conditions module = keyswords of the diseases in the trial
        #### keywords = keywords of the trial
        if "conditionsModule" in protocol_section:
            conditions_module = protocol_section["conditionsModule"]
            clinical_trial.conditions = conditions_module.get("conditions",[])
            clinical_trial.keywords = conditions_module.get("keywords",[])


        if "designModule" in protocol_section:
            design_module = protocol_section["designModule"]    
            clinical_trial.study_type = design_module.get("studyType","")

            clinical_trial.phases = design_module.get("phases",["NA"])
            if clinical_trial.phases == "[]" or clinical_trial.phases == "":
                clinical_trial.phases = ["NA"]
            
            
            clinical_trial.mask_type = design_module.get("designInfo",{}).get("maskingInfo",{}).get("masking","")
            
            # get the enrollment count from the last version of the trial + the theorical enrollment count from the very 
            # first version of the trial
            clinical_trial.last_enrollment_count = design_module.get("enrollmentInfo",{}).get("count",0)
            clinical_trial.theorical_enrollment_count = get_theorical_enrollment_count(clinical_trial.nct_id)
            
            ## interventional/observational trials have different variables names, format them to be the same
            
            if clinical_trial.study_type == "OBSERVATIONAL":
                clinical_trial.interventionModel = design_module.get("designInfo",{}).get("observationalModel","UNKNOWN")
                clinical_trial.intervention_type = "NA FOR OBSERVATIONAL"
                clinical_trial.timePerspective= design_module.get("designInfo",{}).get("timePerspective","OTHER")
                clinical_trial.randomized = "NA FOR OBSERVATIONAL"
                clinical_trial.primaryPurpose = "OBSERVATIONAL"
                
                
            if clinical_trial.study_type == "INTERVENTIONAL":
                clinical_trial.interventionModel = design_module.get("designInfo",{}).get("interventionModel","UNKNOWN")
                clinical_trial.randomized = design_module.get("designInfo",{}).get("allocation","")
                clinical_trial.timePerspective = "PROSPECTIVE"
                try:
                    clinical_trial.intervention_type = protocol_section["armsInterventionsModule"]["interventions"][0]["type"]
                except:
                    clinical_trial.intervention_type = "UNKNOWN"
                clinical_trial.primaryPurpose = design_module.get("designInfo",{}).get("primaryPurpose","")           

        # counting the number of groups in the trial with default being at least one (observational trials don't have groups so we set it to 1)
        if "armsInterventionsModule" in protocol_section:
            arms_module = protocol_section["armsInterventionsModule"]
            arm_groups = arms_module.get("armGroups", [])
            clinical_trial.arm_group_count = len(arm_groups)
        else:
            clinical_trial.arm_group_count = 1


            
        if "ipdSharingStatementModule" in protocol_section:
            ipdsharing = protocol_section["ipdSharingStatementModule"].get("availIpds", False)
            if ipdsharing == "YES":
                clinical_trial.availIpds = True
            else :
                clinical_trial.availIpds = False



        if "eligibilityModule" in protocol_section:
            eligibility_module = protocol_section["eligibilityModule"]
            clinical_trial.healthyVolunteers = eligibility_module.get("healthyVolunteers",0)
            clinical_trial.sex = eligibility_module.get("sex","")
            
            
            # get the value of age as an integer and the metrics as a string
            # Get minimumAge and maximumAge from eligibility_module
            min_age_str = eligibility_module.get("minimumAge", "")
            max_age_str = eligibility_module.get("maximumAge", "")
            # Split the values at the space
            min_age_parts = min_age_str.split()
            max_age_parts = max_age_str.split()
            # Convert the left side to an integer and assign to minimumAge and maximumAge
            clinical_trial.minimumAge = int(min_age_parts[0]) if min_age_parts else None
            clinical_trial.maximumAge = int(max_age_parts[0]) if max_age_parts else None
            # Take the right side of the two values, add them together and assign to AgeMetric
            clinical_trial.AgeMetric = (min_age_parts[1] if len(min_age_parts) > 1 else "") + (max_age_parts[1] if len(max_age_parts) > 1 else "")
                        
        
            clinical_trial.stdAges = eligibility_module.get("stdAges",[])
            eligibility_criteria = eligibility_module.get("eligibilityCriteria", "")
            clinical_trial.eligibility = eligibility_criteria
            #insert_eligibility_criteria(clinical_trial,conn)


        if "contactsLocationsModule" in protocol_section:
            contacts_module = protocol_section["contactsLocationsModule"]
            locations = contacts_module.get("locations", [])
            nct_id = data["protocolSection"]["identificationModule"].get("nctId")
            insert_locations(locations, nct_id,clinical_trial.startDate, conn)
            
            
            '''if 'overallOfficials' in contacts_module:
                overall_officials = contacts_module['overallOfficials']
                try :
                    clinical_trial.facility = overall_officials[0]['affiliation']
                except:
                    clinical_trial.facility = ""
                try:
                    clinical_trial.name = overall_officials[0]['name']
                except:
                    clinical_trial.name = ""
                try:
                    clinical_trial.role = overall_officials[0]['role']
                except:
                    clinical_trial.role = ""
                insert_overall_official_locations(clinical_trial, conn)  '''

    if "hasResults" in data:
        clinical_trial.has_results = data["hasResults"]


    if "resultsSection" in data and (clinical_trial.has_results==1):
        periods = data["resultsSection"]["participantFlowModule"]["periods"]
        for period in periods:
            for milestone in period['milestones']:
                # Use 'period' variable here instead of 'data['periods']'
                periods_df = pd.json_normalize(period, record_path=['milestones', 'achievements'], 
                            meta=[['title'], ['milestones', 'type']], 
                            meta_prefix='parent_', errors='ignore')

                 # Filter the DataFrame
                periods_df = periods_df[periods_df['parent_milestones.type'].isin(["STARTED", "COMPLETED", "NOT COMPLETED"])]

                ###Ajout d'une ligne avec l'ensemble des groupes par nctid et par milestone type
                try :
                    periods_df['numSubjects'] = pd.to_numeric(periods_df['numSubjects'])
                except: periods_df['numSubjects'] = 0
                # Group by 'parent_milestones.type' and sum 'numSubjects'
                grouped_df = periods_df.groupby('parent_milestones.type')['numSubjects'].sum().reset_index()

                # Add 'All' to 'groupId' column
                grouped_df['groupId'] = 'All'
                grouped_df['parent_title'] = period['title']

                # Append the new DataFrame to the original one
                periods_df = pd.concat([periods_df, grouped_df], ignore_index=True)
                periods_df["nct_id"] = clinical_trial.nct_id
                periods_df["overallStatus"] = clinical_trial.overallStatus

            try:
                for milestone in period['dropWithdraws']:
                        # Use 'period' variable here instead of 'data['periods']'
                        dropout_df = pd.json_normalize(period, record_path=['dropWithdraws', 'reasons'], 
                                    meta=[['type'], ['dropWithdraws', 'type']], 
                                    meta_prefix='parent_', errors='ignore')
                        dropout_df["nct_id"] = clinical_trial.nct_id
                        dropout_df["overallStatus"] = clinical_trial.overallStatus
                       
                        
            except:
                continue
            dropout_df["nct_id"] = clinical_trial.nct_id
            dropout_df["overallStatus"] = clinical_trial.overallStatus
            dropout_df["why_stopped"] = clinical_trial.why_stopped
            
            
            insert_dropout(dropout_df,conn)

    if "resultsSection" in data:
        results_section = data["resultsSection"]

        if "baselineCharacteristicsModule" in results_section:
            baseline_characteristics_module = results_section["baselineCharacteristicsModule"]

            denoms = baseline_characteristics_module.get("denoms",[])
            #### .get() marche que pour les dict pour les listes faut faire ca :
            ####counts = [item.get("counts") for item in denoms]

            #If denoms is a list of dictionaries and each dictionary has a 'counts' key that is a dictionary with 
            #'groupId' and 'value' keys, you can normalize it like this:
            counts_df = pd.json_normalize(denoms, record_path=['counts'])
            counts_df = counts_df.rename(columns={'value': 'nb_de_patients'})

            ###This code first gets the first dictionary in the 'measures' list, then gets the first dictionary in the 'classes' list of that dictionary,
            ### then gets the first dictionary in the 'categories' list of that dictionary,
            ###and finally gets the 'measurements' key of that dictionary.
            measures = baseline_characteristics_module.get("measures",[])
            #measurements = measures[0]['classes'][0]['categories'][0]['measurements']

            # Initialize the variable
            classes = None

            # Iterate over the 'measures' list
            for item in measures:
                # Check if the 'title' key is 'Sex: Female, Male'
                if item.get('title') == 'Sex: Female, Male':
                    # If it is, get the 'classes' list
                    classes = item.get('classes')

            # If 'classes' is not None, get the 'categories' list from the first dictionary
            if classes is not None:
                categories = classes[0].get('categories')
            else:
                categories = None

            if categories is not None:
                try:
                    if categories is not None:

                        # Normalize the 'measurements' list
                        sexe_df = pd.json_normalize(data=categories, record_path=['measurements'], meta=['title'])
                        # Pivot sexe_df
                        pivoted_df = sexe_df.pivot(index='groupId', columns='title', values='value')

                        # Reset the index
                        pivoted_df = pivoted_df.reset_index()

                        sexe_df = pivoted_df

                        #merge sexe_df and counts_df on "groupId"
                        merged_df = pd.merge(sexe_df, counts_df, on='groupId')

                except Exception as e:
                    # sometimes there is a dictionnary just saying they didnt bother counting each sex instead of not putting
                    # a stupid dictionnary in the first place
                    print(f"An error occurred: {e} for {clinical_trial.nct_id}")
                    merged_df = counts_df
                    merged_df["Female"] = 0
                    merged_df["Male"] = 0
                
            else:
                merged_df = counts_df
                merged_df["Female"] = 0
                merged_df["Male"] = 0
                

            #### Resoudre le pb que ici la derniere valeur est = à l'ensemble des groupes mais c pas pareil dans
            #### denom, donc replace la derniere valeur groupId par "All" si elle est pas unique

            # If counts_df has only one row
            if len(merged_df) == 1:
                # Rename 'groupId' value to 'FG000'
                merged_df.loc[0, 'groupId'] = 'FG000'
                # Duplicate the row with 'groupId' value = 'All'
                new_row = merged_df.copy()
                new_row['groupId'] = 'All'
                merged_df = pd.concat([merged_df, new_row])

            # If counts_df has more than one row
            else:
                # Replace 'BG' with 'FG' in 'groupId'
                merged_df['groupId'] = merged_df['groupId'].str.replace('BG', 'FG')
                # Replace the last row's 'groupId' value with 'All'
                merged_df.loc[merged_df.index[-1], 'groupId'] = 'All'

            
            # Merge counts_df and periods_df on 'groupId'
            fullmerged_df = pd.merge(periods_df, merged_df, on='groupId')
            
            insert_baseline_characteristics(fullmerged_df,conn)

    if 'referencesModule' in protocol_section :
        references = protocol_section['referencesModule'].get('references',{})
        if references:
            for reference in references:
                insert_references(reference,clinical_trial.nct_id,conn)
        

    if "derivedSection" in data:
        derived_section = data["derivedSection"]

        if "conditionBrowseModule" in derived_section:
            conditionbrowse_module = derived_section["conditionBrowseModule"]
            
    
            clinical_trial.condition_meshes = conditionbrowse_module.get("meshes",{})
            insert_mesh(clinical_trial.condition_meshes, clinical_trial.nct_id, conn,"condition_mesh")
            

            clinical_trial.condition_ancestors = conditionbrowse_module.get("ancestors",{})
            insert_mesh(clinical_trial.condition_ancestors, clinical_trial.nct_id, conn,"condition_ancestors")
            
            clinical_trial.condition_browseBranches = conditionbrowse_module.get("browseBranches",{})
            insert_mesh(clinical_trial.condition_browseBranches, clinical_trial.nct_id, conn,"condition_browseBranches")
            
            

        if "interventionBrowseModule" in derived_section:
            interventionbrowse_module = derived_section["interventionBrowseModule"]
            clinical_trial.intervention_meshes = interventionbrowse_module.get("meshes",{})
            insert_mesh(clinical_trial.intervention_meshes, clinical_trial.nct_id, conn,"intervention_mesh")

            clinical_trial.intervention_ancestors = interventionbrowse_module.get("ancestors",{})
            insert_mesh(clinical_trial.intervention_ancestors, clinical_trial.nct_id, conn,"intervention_ancestors")

            clinical_trial.intervention_browseBranches = interventionbrowse_module.get("browseBranches",{})
            insert_mesh(clinical_trial.intervention_browseBranches, clinical_trial.nct_id, conn,"intervention_browseBranches")

        if "outcomesModule" in protocol_section:
            outcomes_module = protocol_section["outcomesModule"]
            clinical_trial.nb_primaryOutcomes = len(outcomes_module.get("primaryOutcomes", []))
            clinical_trial.nb_secondaryOutcomes = len(outcomes_module.get("secondaryOutcomes", []))
            
    return clinical_trial





def insert_into_database(clinical_trial,conn):
    

    try:
        
        c = conn.cursor()
        
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS clinicaltrial (
                nct_id VARCHAR(50) PRIMARY KEY,
                is_Rare BOOLEAN,
                lastUpdateSubmitDate DATE,
                org_name TEXT,
                org_study_class VARCHAR(25),
                responsibleParty VARCHAR(25),
                primaryCompletionDate DATE,
                overallStatus TEXT,
                hasExpandedAccess BOOLEAN,
                startDate DATE,
                completionDate DATE,
                lead_sponsor TEXT,
                sponsor_class TEXT,
                oversighthasdmc BOOLEAN,
                isfdaregulateddrug BOOLEAN,
                conditions TEXT,
                keywords TEXT,
                study_type TEXT,
                phases TEXT,
                interventionModel TEXT,
                primaryPurpose TEXT,
                mask_type TEXT,
                theorical_enrollment_count INT,
                last_enrollment_count INT,
                arm_group_count INT,
                intervention_type TEXT,
                timePerspective TEXT,
                healthyVolunteers BOOLEAN,
                sex TEXT,
                minimumAge TEXT,
                maximumAge TEXT,
                AgeMetric VARCHAR(15),
                stdAges TEXT,
                has_results BOOLEAN,
                why_stopped TEXT,
                randomized TEXT,
                nb_secondaryOutcomes INT,
                nb_primaryOutcomes INT,
                total_centers INT,
                number_of_collaborators INT,
                country TEXT,
                continent TEXT
            ) CHARACTER SET utf8mb4;
        ''')

        c.execute('''
            INSERT INTO clinicaltrial 
            (nct_id, is_Rare,lastUpdateSubmitDate,org_name, org_study_class, responsibleParty, primaryCompletionDate, overallStatus, hasExpandedAccess, 
            startDate, completionDate, lead_sponsor, sponsor_class, oversighthasdmc, 
            isfdaregulateddrug, conditions, keywords, study_type, phases, interventionModel, 
            primaryPurpose, mask_type, theorical_enrollment_count, last_enrollment_count, arm_group_count, intervention_type, timePerspective, 
            healthyVolunteers, sex, minimumAge, maximumAge, AgeMetric, stdAges, has_results, why_stopped, randomized, 
            nb_secondaryOutcomes, nb_primaryOutcomes)
            VALUES ( %s,%s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            is_Rare = VALUES(is_Rare),
            lastUpdateSubmitDate = VALUES(lastUpdateSubmitDate),
            org_name = VALUES(org_name), 
            org_study_class = VALUES(org_study_class), 
            responsibleParty = VALUES(responsibleParty), 
            primaryCompletionDate = VALUES(primaryCompletionDate), 
            overallStatus = VALUES(overallStatus), 
            hasExpandedAccess = VALUES(hasExpandedAccess), 
            startDate = VALUES(startDate), 
            completionDate = VALUES(completionDate), 
            lead_sponsor = VALUES(lead_sponsor), 
            sponsor_class = VALUES(sponsor_class), 
            oversighthasdmc = VALUES(oversighthasdmc), 
            isfdaregulateddrug = VALUES(isfdaregulateddrug), 
            conditions = VALUES(conditions), 
            keywords = VALUES(keywords), 
            study_type = VALUES(study_type), 
            phases = VALUES(phases), 
            interventionModel = VALUES(interventionModel), 
            primaryPurpose = VALUES(primaryPurpose), 
            mask_type = VALUES(mask_type), 
            theorical_enrollment_count = VALUES(theorical_enrollment_count), 
            last_enrollment_count = VALUES(last_enrollment_count), 
            arm_group_count = VALUES(arm_group_count),
            intervention_type = VALUES(intervention_type), 
            timePerspective = VALUES(timePerspective), 
            healthyVolunteers = VALUES(healthyVolunteers), 
            sex = VALUES(sex), 
            minimumAge = VALUES(minimumAge), 
            maximumAge = VALUES(maximumAge), 
            AgeMetric = VALUES(AgeMetric), 
            stdAges = VALUES(stdAges), 
            has_results = VALUES(has_results), 
            why_stopped = VALUES(why_stopped), 
            randomized = VALUES(randomized), 
            nb_secondaryOutcomes = VALUES(nb_secondaryOutcomes), 
            nb_primaryOutcomes = VALUES(nb_primaryOutcomes)
        ''', (clinical_trial.nct_id, clinical_trial.is_Rare, clinical_trial.lastUpdateSubmitDate,clinical_trial.org_name, clinical_trial.org_study_class,
              clinical_trial.responsibleParty, clinical_trial.primaryCompletionDate, clinical_trial.overallStatus, 
               clinical_trial.hasExpandedAccess, clinical_trial.startDate, 
              clinical_trial.completionDate, clinical_trial.lead_sponsor, 
              clinical_trial.sponsor_class, clinical_trial.oversighthasdmc, clinical_trial.isfdaregulateddrug, 
              clinical_trial.conditions, clinical_trial.keywords, clinical_trial.study_type, clinical_trial.phases, 
              clinical_trial.interventionModel, clinical_trial.primaryPurpose, clinical_trial.mask_type, 
              clinical_trial.theorical_enrollment_count, clinical_trial.last_enrollment_count, clinical_trial.arm_group_count, 
              clinical_trial.intervention_type, clinical_trial.timePerspective, clinical_trial.healthyVolunteers, 
              clinical_trial.sex,  clinical_trial.minimumAge, clinical_trial.maximumAge, 
              clinical_trial.AgeMetric, clinical_trial.stdAges, clinical_trial.has_results, clinical_trial.why_stopped, 
              clinical_trial.randomized, clinical_trial.nb_secondaryOutcomes, clinical_trial.nb_primaryOutcomes))
        # Commit changes and close connection
        conn.commit()
        c.close()
        #print("Inserted into MariaDB database:", clinical_trial.nct_id)
    except mariadb.Error as e:
        print(f"Error: {e}/n for {clinical_trial.nct_id}")
    
        

def updating_values():
    
    # Connect to MariaDB
    conn = mariadb.connect(
        host="localhost",
        user="root",
        password="root",
        port=3306,
        database=database
    )


    print("Setting Rare Diseases trials bool")
    # Create cursor
    cursor = conn.cursor()

    # Update is_Rare in the clinical_trial table for nct_id values with mesh_id "Rare" or "D035583"
    cursor.execute("""
        UPDATE clinicaltrial
        SET is_Rare = TRUE
        WHERE nct_id IN (
            SELECT DISTINCT nct_id
            FROM termes_mesh
            WHERE mesh_id = 'Rare' OR mesh_id = 'D035583'
        )
    """)

    # Commit the changes
    conn.commit()

    print('updating values in collaborators table')
    #update row_count in table collaborators
    c = conn.cursor()
    
    # Fetch distinct nct_id values
    cursor.execute("SELECT DISTINCT nct_id FROM collaborators WHERE number_of_collaborators IS NULL")
    nct_ids = cursor.fetchall()

    for nct_id in nct_ids:
        # Count the number of rows for each nct_id
        cursor.execute("""
            SELECT COUNT(*) 
            FROM collaborators 
            WHERE nct_id = ?
        """, (nct_id[0],))
        count = cursor.fetchone()[0]

        # Update the number_of_collaborators column
        cursor.execute("""
            UPDATE collaborators 
            SET number_of_collaborators = ? 
            WHERE nct_id = ? AND number_of_collaborators IS NULL
        """, (count, nct_id[0]))

        conn.commit()

    # Update number_of_collaborators in the clinical_trial table using only the first row with the same nct_id
    c.execute("""
    UPDATE clinicaltrial
    JOIN (
        SELECT DISTINCT nct_id, number_of_collaborators
        FROM collaborators
    ) AS rc ON clinicaltrial.nct_id = rc.nct_id
    SET clinicaltrial.number_of_collaborators = rc.number_of_collaborators
    WHERE clinicaltrial.number_of_collaborators IS NULL
""")
    conn.commit() 
    c.close()

    ### Query to retrieve unique countries from locations table
    ## load continent/country table
    # fetch the table locations
    print("Replacing alt country names with standard country names")
    query = "SELECT distinct country,continent FROM gdp_details"
    cursor.execute(query)
    locations = cursor.fetchall()
    locations_df = pd.DataFrame(locations, columns=['country', 'continent'])
    country_continent_dict = pd.Series(locations_df['continent'].values, index=locations_df['country']).to_dict()


    query = """SELECT DISTINCT country 
    FROM locations 
    WHERE continent IS NULL;"""

    cursor.execute(query)
    rows = cursor.fetchall()

    # dictionnary of alt country names
    alt_names ={"Korea, Republic of":"South Korea","Korea, Democratic People's Republic of":"South Korea","Russian Federation":"Russia","Iran, Islamic Republic of":"Iran",
                "Moldova, Republic of":"Moldova","Congo, The Democratic Republic of the":"Democratic Republic of the Congo",
                "Syrian Arab Republic":"Syria","Côte d'Ivoire":"Ivory Coast","Côte D'Ivoire":"Ivory Coast","Cote d'Ivoire":"Ivory Coast","Macedonia, the former Yugoslav Republic of":"North Macedonia",
                "Former Serbia and Montenegro":"Serbia","Palestinian Territory, occupied": "Palestine","Libyan Arab Jamahiriya":"Libya",
                "American Samoa":"Samoa","Brunei Darussalam":"Brunei","Viet Nam":"Vietnam","Lao People's Democratic Republic":"Laos",
                "Tanzania, United Republic of":"Tanzania","Venezuela, Bolivarian Republic of":"Venezuela",
                "Macedonia, The Former Yugoslav Republic of":"North Macedonia","Liechtenstein":"Switzerland","Congo":"Republic of the Congo",
                "Former Yugoslavia":"Serbia","Faroe Islands":"Denmark","Virgin Islands (U.S.)":"United States","Netherlands Antilles":"Netherlands",
                "Palestinian Territories, Occupied":"Palestine", "Holy See (Vatican City State)":"Italy","Aland Islands":"Finland","Jersey":"United Kingdom",
                "Northern Mariana Islands":"United States","Gibraltar":"United Kingdom","Bonaire, Sint Eustatius and Saba":"Netherlands","Saint Martin":"France",
                "United States Minor Outlying Islands":"United States","Federated States of Micronesia":"Micronesia","Guam":"United States",
                "Martinique":"France","Réunion": "France","Swaziland":"Eswatini","Mayotte":"France","Guadeloupe":"France","French Guiana":"France"}

    # Process each row to update the continent in the locations DataFrame
    for row in rows:
        country_name = row[0]
        if country_name in alt_names:
            alt_country_name = alt_names[country_name]
            continent = country_continent_dict.get(alt_country_name)
        else:
            alt_country_name = country_name
            continent = country_continent_dict.get(country_name)  # Get continent from the dictionary

        if continent:
            print(f"{country_name}: {continent}")
            update_query = "UPDATE locations SET continent = %s, country = %s WHERE country = %s"
            cursor.execute(update_query, (continent, alt_country_name, country_name))
            conn.commit()

    cursor.close()
    
    print('Counting total center count for each trials')
    # Create cursor
    cursor = conn.cursor()

    # Fetch the row with the highest value of total_center for each distinct nct_id and update the clinical_trial table
    cursor.execute("""
        UPDATE clinicaltrial
        JOIN (
            SELECT DISTINCT nct_id, total_center
            FROM locations
        ) AS unique_locations ON clinicaltrial.nct_id = unique_locations.nct_id
        SET clinicaltrial.total_centers = unique_locations.total_center
    """)
    conn.commit()

    # Close cursor and connection
    cursor.close()
    print('update locations in other tables for mono center trials')
    # Create cursor
    cursor = conn.cursor()

    # Select rows from locations where total_center = 1 and update clinical_trial and responsibleparty_location tables
    cursor.execute("""
        UPDATE clinicaltrial
        JOIN (
            SELECT nct_id, country, continent
            FROM locations
            WHERE total_center = 1
        ) AS single_center_locations ON clinicaltrial.nct_id = single_center_locations.nct_id
        SET clinicaltrial.country = single_center_locations.country,
            clinicaltrial.continent = single_center_locations.continent
    """)

    cursor.execute("""
        UPDATE responsibleparty_location
        JOIN (
            SELECT nct_id, country, continent
            FROM locations
            WHERE total_center = 1
        ) AS single_center_locations ON responsibleparty_location.nct_id = single_center_locations.nct_id
        SET responsibleparty_location.country = single_center_locations.country,
            responsibleparty_location.continent = single_center_locations.continent
    """)

    # Commit the changes
    conn.commit()

    # Close cursor and connection
    cursor.close()
    conn.close()
        
        
def last_update_date(trial,conn):
    c = conn.cursor()
    c.execute("SELECT lastUpdateSubmitDate FROM clinicaltrial WHERE nct_id = ?", (trial.nct_id,))
    last_update = c.fetchone()
    if last_update is None:
        print("New trial", trial.nct_id)
        return True
    # convert the string YYY-MM-DD to datetime.date
    current_date = datetime.datetime.strptime(trial.lastUpdateSubmitDate, '%Y-%m-%d').date()
    #print(f"last update date for {trial.nct_id} is {last_update[0]} vs current date {current_date}")
    if last_update[0]<current_date:
        print(f"{trial.nct_id} : updating version {last_update[0]} to {current_date}")
        return True
    if last_update[0]>=current_date:
        print("last update date already up to date for", trial.nct_id)
        return False
    else:
        print("No last update date found for", trial.nct_id)
        return True


size = 1000

def fetch_content(page_token):
    #url = 'https://clinicaltrials.gov/api/v2/studies?format=json&pageSize=1000'
    url = f'https://clinicaltrials.gov/api/v2/studies?format=json&filter.advanced=AREA%5BStartDate%5DRANGE%5B2017%2CMAX%5D&sort=LastUpdatePostDate&countTotal=true&pageSize={size}'
    if page_token:
        url += f'&pageToken={page_token}'

    response = requests.get(url)
    if response.status_code == 200:
        json_response = response.json()
        next_page_token = json_response.get("nextPageToken")
        
        ##### insert in db
        for data in json_response["studies"]:
            #time.sleep(0.1)

            conn = mariadb.connect(
            host = "localhost",
            user = "root",
            password = "root",
            port=3306,
            database=database)
            trial = parse_json(data,conn)
            if trial:
                insert_into_database(trial,conn)
        conn.commit() 
        

        conn.close()
        

        return next_page_token
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

def get_total_trials():
    url = f'https://clinicaltrials.gov/api/v2/studies?format=json&filter.advanced=AREA%5BStartDate%5DRANGE%5B2017%2CMAX%5D&sort=LastUpdatePostDate&countTotal=true&pageSize={size}'
    response = requests.get(url)
    if response.status_code == 200:
        json_response = response.json()
        total_count = (json_response.get("totalCount"))
            
    return total_count

def main():
    # Set up argparse
    parser = argparse.ArgumentParser(description="Fetch data from clinical trials.")
    parser.add_argument('--reset', action='store_true', help='Fetch all pages if set; otherwise, fetch only the first page.')
    parser.add_argument('--page_token', type=str, help='Page token for pagination control.')
    parser.add_argument('--token_nb', type=int, help='Page token for pagination control.')
    args = parser.parse_args()
    
    # first creation of the table : insert gdp_details
    if args.reset:
        conn = mariadb.connect(
            host = "localhost",
            user = "root",
            password = "root",
            port=3306)
        c = conn.cursor()
        # create database if it doesn't exist
        c.execute(f'CREATE DATABASE IF NOT EXISTS {database}')
        csv_file_path = 'gdp_details.csv'
        df = pd.read_csv(csv_file_path)
        engine = create_engine(f'mysql+pymysql://root:root@localhost:3306/{database}')
        df.to_sql('gdp_details', con=engine, if_exists='replace', index=False)
        print("GDP_details table created")
        c.close()
    if args.page_token:
        page_token = args.page_token
    else:
        page_token = None
    if args.token_nb:
        i = args.token_nb
    else : i =1
    
    total_count = get_total_trials()
    while True:    
        print(f"Current chunk N°{i} / {int(total_count/size)}")
        
        next_page_token = fetch_content(page_token)
        #updating collab count and country/continent
        #updating_values()
        if not next_page_token or not args.reset :
            break
        page_token = next_page_token
        print(f'current {i}th token:{page_token}')
        i += 1

if __name__ == "__main__":
    main()
updating_values()





### regex edit Tcptimedwaitdelay & Maxuserport 
### https://topic.alibabacloud.com/a/cant-connect-to-mysql-server-on-localhost-10048_1_41_32269599.html
