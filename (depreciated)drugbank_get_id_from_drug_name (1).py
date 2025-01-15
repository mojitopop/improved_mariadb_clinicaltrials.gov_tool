import pandas as pd
import requests
import time
import mariadb



"""
This script fetches DrugBank IDs for drug names from the DrugBank website without the use of an API key.
For each different drug name, make a search query, if the query returns a results page, then the ID is not found.
If the query redirects to a drug page, then the ID is found and we extract the ID from the end of the URL.

"""


# Function to fetch the drug ID from DrugBank
import requests

import requests

def fetch_drug_id(drug_name):
    base_url = "https://go.drugbank.com/unearth/q?searcher=drugs&query={}"
    url = base_url.format(drug_name)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    
    try:
        response = requests.get(url, headers=headers)
        print('Response status code:', response.status_code)
        print('Response history:', response.history)
        
        if response.status_code == 403:
            print('Access forbidden: 403 error')
            return "forbidden"
        
        if response.history:
            final_url = response.url
            drug_id = final_url.split('/')[-1]
            print('Drug ID:', drug_id)
            return drug_id
        else:
            print('No redirection history, returning "0"')
            return "0"
    except Exception as e:
        print(f"Error fetching data for {drug_name}: {e}")
        return "error"


database = "ct_final"
connection = mariadb.connect(
host = "localhost",
user = "root",
password = "root",
port=3306,
database=database)
try:
    with connection.cursor() as cursor:
        # Query to get drug names sorted by frequency
        sql_query = """
        SELECT druginfo, COUNT(*) AS nb_row
        FROM topic_druginfo
        GROUP BY druginfo
        ORDER BY nb_row DESC
        """
        cursor.execute(sql_query)
        results = cursor.fetchall()
        
        # Process each drug name and fetch DrugBank ID
        i=0
        for row in results:
            i+=1
            print(f"Processing {i}/{len(results)}: {row[0]}")
            drug_name = row[0]
            drug_id = fetch_drug_id(drug_name)
            
            
            # Update the database for all occurrences of this drug name
            update_query = """
            UPDATE topic_druginfo
            SET drugbank_id = %s
            WHERE druginfo = %s
            """
            cursor.execute(update_query, (drug_id, drug_name))
            
            # Sleep to avoid hitting the DrugBank too fast
            time.sleep(.5)
        
        # Commit the updates
        connection.commit()

finally:
    # Close the database connection
    connection.close()

print("DrugBank IDs have been fetched and updated successfully.")
