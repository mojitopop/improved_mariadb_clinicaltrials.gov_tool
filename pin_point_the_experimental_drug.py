import pandas as pd
import pymysql
import pandas as pd
from lxml import html
import time
import pandas as pd
import json
import requests
import mariadb
import warnings
warnings.filterwarnings("ignore")
database = "ct_final"

"""
Script to extract and process experimental drugs from clinical trial data.
Fetch all the different armgroups + all the different treatments combinations
Cross references the differents groups that are not classified as comparators and extract the drugs 
Determine if the protocol is experimental only, experimental vs placebo, experimental vs active comparator

If the name of the experimental drug is unclear, a column with all the others drugs name is inserted as potential match and to be manually checked
"""

conn = mariadb.connect(
host = "localhost",
user = "root",
password = "root",
port=3306,
database=database)

c = conn.cursor()
# SQL query for creating table if it doesn't exist
c.execute("""CREATE TABLE IF NOT EXISTS study_exp_drugs (
    nct_id VARCHAR(50),
    Intervention_type VARCHAR(100),
    name VARCHAR(250),
    Drug_type VARCHAR(150),
    other_names TEXT,
    potential_match TEXT,
    experimental_only TINYINT(1) DEFAULT 0,
    exp_against_placebo TINYINT(1) DEFAULT 0,
    exp_against_active_comp TINYINT(1) DEFAULT 0,
    
    PRIMARY KEY (nct_id, name))""")
    
    
def process_intervention_data(data,conn,nct_id,topic_druginfo):
    """
    This function processes JSON data containing information about arm groups and interventions.
    It extracts relevant information, handles missing values, and returns a DataFrame with the structured data.

    Parameters:
    data (dict): JSON data structure containing arm groups and interventions.

    Returns:
    pd.DataFrame: A DataFrame containing the processed data.
    """

    # Prepare a list to hold the rows of extracted data
    rows = []

    # Extract data from armGroups and populate the initial DataFrame
    try:
        # Iterate over each arm group in the data
        for arm in data['armsInterventionsModule']['armGroups']:
            # Safely extract the 'label', 'type', and 'interventionNames' from each arm
            label = arm.get('label', None)  # Default to None if not found
            arm_type = arm.get('type', None)  # Default to None if not found
            intervention_names = arm.get('interventionNames', [])  # Default to empty list if not found
            
            # Loop through each intervention name in the interventionNames list
            for intervention in intervention_names:
                try:
                    # Split the intervention into type and name using ": " as the separator
                    intervention_type, intervention_name = intervention.split(": ")
                except ValueError:
                    # Handle cases where the intervention string does not contain ": "
                    # Set intervention_type to None and use the full string as intervention_name
                    intervention_type = None
                    intervention_name = intervention  # Use the whole string if no split
                
                # Append a dictionary of the extracted values to the rows list
                rows.append({
                    'label': label,
                    'type': arm_type,
                    'intervention': intervention_name,
                    'intervention_type': intervention_type.upper() if intervention_type else None,  # Convert to uppercase if not None
                    'otherNames': None  # Placeholder for other names
                })
    except KeyError as e:
        # Catch and report any KeyError that occurs while processing armGroups
        print(f"Key error while processing armGroups: {e}")

    # Convert the list of rows into a DataFrame
    df = pd.DataFrame(rows)
    input(df)

    # Now, iterate through the interventions and update the DataFrame
    try:
        for intervention in data['armsInterventionsModule']['interventions']:
            # Extract intervention type and arm group labels
            intervention_type = intervention.get('type', None)  # Default to None if not found
            arm_group_labels = intervention.get('armGroupLabels', [])  # Default to empty list if not found
            other_names = intervention.get('otherNames', None)  # Extract 'otherNames', default to None if not found
            
            # Check each armGroupLabel and update the corresponding rows in the DataFrame
            for label in arm_group_labels:
                # Find the matching rows based on label and intervention name
                matching_rows = (df['label'] == label) & (df['intervention'] == intervention.get('name', None))
                
                # If there are matching rows, update the 'otherNames' column with a joined string of other names
                if matching_rows.any():  # Ensure that matching rows exist before updating
                    df.loc[matching_rows, 'otherNames'] = ', '.join(other_names) if other_names else None

    except KeyError as e:
        # Catch and report any KeyError that occurs while processing interventions
        print(f"Key error while processing interventions: {e}")

    # Print the updated DataFrame
    df1 = df.iloc[:, 1:]
    # remove duplicate rows
    df2=df1.drop_duplicates()
    df2['nct_id'] = nct_id
    df2['potential_match'] = topic_druginfo
    input(df2)
    # insert data into the database
    c = conn.cursor()
    for index, row in df2.iterrows():
        c.execute(
            """INSERT INTO study_exp_drugs (nct_id, Intervention_type, name, Drug_type, other_names, potential_match)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE nct_id = VALUES(nct_id), Intervention_type = VALUES(Intervention_type),
            name = VALUES(name), Drug_type = VALUES(Drug_type), other_names = VALUES(other_names), potential_match = VALUES(potential_match)""",
            (row['nct_id'], row['type'], row['intervention'], row['intervention_type'], row['otherNames'], row['potential_match'])) 
        conn.commit()
    c.close()
    return df2


def fetch_and_update(c):
    c.execute("""SELECT DISTINCT nct_id FROM clinicaltrial_extra WHERE nct_id NOT IN (SELECT DISTINCT nct_id FROM study_exp_drugs) AND topic_druginfo NOT LIKE "[]"
              ORDER BY RAND()
                LIMIT 5000;""")
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
            if "topics" in data:
                
                topic_drugInfo =  [item['name'] for item in data.get('topics', {}).get('drugInfo', [])]
                if not topic_drugInfo:
                    topic_drugInfo = ['No drug info'] #default value
            
                if 'study' in data:
                    data_study = data['study']["protocolSection"]
                    if 'armsInterventionsModule' in data_study:
                        topic_drugInfox = ', '.join(topic_drugInfo)
                        
                        process_intervention_data(data_study,conn,nct_id,topic_drugInfox)


while True:
    fetch_and_update(c)

    #### post process fixing the data
    """
    UPDATE study_exp_drugs
    SET Drug_type = 'DRUG' WHERE Drug_type IS NULL AND NAME LIKE 'drug:%';
    UPDATE study_exp_drugs
    SET Drug_type = 'BIOLOGICAL' WHERE Drug_type IS NULL AND NAME LIKE 'biological:%';
    UPDATE study_exp_drugs
    SET Drug_type = 'COMBINATION PRODUCT' WHERE Drug_type IS NULL AND NAME LIKE 'combination product:%';
    UPDATE study_exp_drugs
    SET Drug_type = 'OTHER' WHERE Drug_type IS NULL AND NAME LIKE 'other:%';
    UPDATE study_exp_drugs
    SET Drug_type = 'BEHAVIORAL' WHERE Drug_type IS NULL AND NAME LIKE 'behavioral:%';
    UPDATE study_exp_drugs
    SET Drug_type = 'PROCEDURE' WHERE Drug_type IS NULL AND NAME LIKE 'procedure:%';
    UPDATE study_exp_drugs
    SET Drug_type = 'RADIATION' WHERE Drug_type IS NULL AND NAME LIKE 'radiation:%';
    UPDATE study_exp_drugs
    SET Drug_type = 'DIETARY SUPPLEMENT' WHERE Drug_type IS NULL AND NAME LIKE 'dietary supplement:%';
    UPDATE study_exp_drugs
    SET Drug_type = 'DEVICE' WHERE Drug_type IS NULL AND NAME LIKE 'device:%';
    UPDATE study_exp_drugs
    SET Drug_type = 'GENETIC' WHERE Drug_type IS NULL AND NAME LIKE 'genetic:%';"""

    ## update exp only, exp vs sham, exp vs placebeo
    """-- Update experimental_only field
    UPDATE study_exp_drugs sd
    JOIN (
        SELECT nct_id,
            CASE 
                WHEN COUNT(DISTINCT Intervention_type) = 1 
                    AND MIN(Intervention_type) = 'EXPERIMENTAL' THEN 1
                ELSE 0
            END AS experimental_only
        FROM study_exp_drugs
        GROUP BY nct_id
    ) temp_exp ON sd.nct_id = temp_exp.nct_id
    SET sd.experimental_only = temp_exp.experimental_only;

    -- Update exp_against_placebo field
    UPDATE study_exp_drugs sd
    JOIN (
        SELECT nct_id,
            CASE 
                WHEN SUM(Intervention_type IN ('PLACEBO_COMPARATOR', 'SHAM_COMPARATOR')) > 0 THEN 1
                ELSE 0
            END AS exp_against_placebo
        FROM study_exp_drugs
        GROUP BY nct_id
    ) temp_placebo ON sd.nct_id = temp_placebo.nct_id
    SET sd.exp_against_placebo = temp_placebo.exp_against_placebo;

    -- Update exp_against_active_comp field
    UPDATE study_exp_drugs sd
    JOIN (
        SELECT nct_id,
            CASE 
                WHEN SUM(Intervention_type = 'ACTIVE_COMPARATOR') > 0 THEN 1
                ELSE 0
            END AS exp_against_active_comp
        FROM study_exp_drugs
        GROUP BY nct_id
    ) temp_active ON sd.nct_id = temp_active.nct_id
    SET sd.exp_against_active_comp = temp_active.exp_against_active_comp;
    """
    """Explanation:
    Updating experimental_only:

    This query uses a JOIN to combine data from the same table and updates the experimental_only column to 1 if for a given nct_id, all values of Intervention_type are exclusively 'EXPERIMENTAL'.
    It checks this by ensuring that there is only one distinct Intervention_type, and that value is 'EXPERIMENTAL'.
    Updating exp_against_placebo:

    This part checks if any Intervention_type for the given nct_id includes 'PLACEBO_COMPARATOR' or 'SHAM_COMPARATOR'. If true, it sets the exp_against_placebo column to 1.
    Updating exp_against_active_comp:

    This section sets exp_against_active_comp to 1 if there exists at least one 'ACTIVE_COMPARATOR' for a given nct_id."""

    # join those 3 bools col to the main table
    """ALTER TABLE clinicaltrial_extra
    ADD COLUMN experimental_only TINYINT(1) DEFAULT 0,
    ADD COLUMN exp_against_placebo TINYINT(1) DEFAULT 0,
    ADD COLUMN exp_against_active_comp TINYINT(1) DEFAULT 0;
    UPDATE clinicaltrial_extra ct
    JOIN (
        SELECT nct_id,
            MAX(experimental_only) AS experimental_only,
            MAX(exp_against_placebo) AS exp_against_placebo,
            MAX(exp_against_active_comp) AS exp_against_active_comp
        FROM study_exp_drugs
        GROUP BY nct_id
    ) sd ON ct.nct_id = sd.nct_id
    SET ct.experimental_only = sd.experimental_only,
        ct.exp_against_placebo = sd.exp_against_placebo,
        ct.exp_against_active_comp = sd.exp_against_active_comp;
    """



    def update_study_exp_drugs(conn):
        cursor = conn.cursor()

        # Update Drug_type based on NAME
        cursor.execute("""
            UPDATE study_exp_drugs
            SET Drug_type = 'DRUG' WHERE Drug_type IS NULL AND NAME LIKE 'drug:%';
        """)
        cursor.execute("""
            UPDATE study_exp_drugs
            SET Drug_type = 'BIOLOGICAL' WHERE Drug_type IS NULL AND NAME LIKE 'biological:%';
        """)
        cursor.execute("""
            UPDATE study_exp_drugs
            SET Drug_type = 'COMBINATION PRODUCT' WHERE Drug_type IS NULL AND NAME LIKE 'combination product:%';
        """)
        cursor.execute("""
            UPDATE study_exp_drugs
            SET Drug_type = 'OTHER' WHERE Drug_type IS NULL AND NAME LIKE 'other:%';
        """)
        cursor.execute("""
            UPDATE study_exp_drugs
            SET Drug_type = 'BEHAVIORAL' WHERE Drug_type IS NULL AND NAME LIKE 'behavioral:%';
        """)
        cursor.execute("""
            UPDATE study_exp_drugs
            SET Drug_type = 'PROCEDURE' WHERE Drug_type IS NULL AND NAME LIKE 'procedure:%';
        """)
        cursor.execute("""
            UPDATE study_exp_drugs
            SET Drug_type = 'RADIATION' WHERE Drug_type IS NULL AND NAME LIKE 'radiation:%';
        """)
        cursor.execute("""
            UPDATE study_exp_drugs
            SET Drug_type = 'DIETARY SUPPLEMENT' WHERE Drug_type IS NULL AND NAME LIKE 'dietary supplement:%';
        """)
        cursor.execute("""
            UPDATE study_exp_drugs
            SET Drug_type = 'DEVICE' WHERE Drug_type IS NULL AND NAME LIKE 'device:%';
        """)
        cursor.execute("""
            UPDATE study_exp_drugs
            SET Drug_type = 'GENETIC' WHERE Drug_type IS NULL AND NAME LIKE 'genetic:%';
        """)

        # Update experimental_only field
        cursor.execute("""
            UPDATE study_exp_drugs sd
            JOIN (
                SELECT nct_id,
                    CASE 
                        WHEN COUNT(DISTINCT Intervention_type) = 1 
                            AND MIN(Intervention_type) = 'EXPERIMENTAL' THEN 1
                        ELSE 0
                    END AS experimental_only
                FROM study_exp_drugs
                GROUP BY nct_id
            ) temp_exp ON sd.nct_id = temp_exp.nct_id
            SET sd.experimental_only = temp_exp.experimental_only;
        """)

        # Update exp_against_placebo field
        cursor.execute("""
            UPDATE study_exp_drugs sd
            JOIN (
                SELECT nct_id,
                    CASE 
                        WHEN SUM(Intervention_type IN ('PLACEBO_COMPARATOR', 'SHAM_COMPARATOR')) > 0 THEN 1
                        ELSE 0
                    END AS exp_against_placebo
                FROM study_exp_drugs
                GROUP BY nct_id
            ) temp_placebo ON sd.nct_id = temp_placebo.nct_id
            SET sd.exp_against_placebo = temp_placebo.exp_against_placebo;
        """)

        # Update exp_against_active_comp field
        cursor.execute("""
            UPDATE study_exp_drugs sd
            JOIN (
                SELECT nct_id,
                    CASE 
                        WHEN SUM(Intervention_type = 'ACTIVE_COMPARATOR') > 0 THEN 1
                        ELSE 0
                    END AS exp_against_active_comp
                FROM study_exp_drugs
                GROUP BY nct_id
            ) temp_active ON sd.nct_id = temp_active.nct_id
            SET sd.exp_against_active_comp = temp_active.exp_against_active_comp;
        """)

        conn.commit()

    def update_clinicaltrial_extra(conn):
        cursor = conn.cursor()


        # Update clinicaltrial_extra with values from study_exp_drugs
        cursor.execute("""
            UPDATE clinicaltrial_extra ct
            JOIN (
                SELECT nct_id,
                    MAX(experimental_only) AS experimental_only,
                    MAX(exp_against_placebo) AS exp_against_placebo,
                    MAX(exp_against_active_comp) AS exp_against_active_comp
                FROM study_exp_drugs
                GROUP BY nct_id
            ) sd ON ct.nct_id = sd.nct_id
            SET ct.experimental_only = sd.experimental_only,
                ct.exp_against_placebo = sd.exp_against_placebo,
                ct.exp_against_active_comp = sd.exp_against_active_comp;
        """)

        conn.commit()

    update_study_exp_drugs(conn)
    update_clinicaltrial_extra(conn)