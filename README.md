# improved_mariadb_clinicaltrials.gov_tool
A script that create a MariaDB db scrapping data from clinicaltrials.gov official API and add some improvement to the accuracy of the results using clinicaltrials.gov record history backend API


![Diagramme EA avec entités en couleur (notation UML) (2)](https://github.com/user-attachments/assets/b79ca393-6a61-4dcb-b97d-2b8154dfbb37)
Overview of some of the tables this script creates. ( doesn't show tables related to cohort baselines characteristics, secondary ID&PMID references)
Red variables represent values that were modified by the scripts


// You need to have all the MariaDB drivers installed beforehand in order to use this

NB : some tables and informations are very redundant, this was done purposely in order to have a fast visual access to all the information needed instead of having to perform a MySQL query every time i wanted to join tables or see real text values instead of foreign keys.

The db schema can be found in the rep

List of changes :
 - regex the lead sponsors / orgas names in order to refine the class OTHER into UNIVERSITY,HOSPITAL and NETWORK
 - format dates to YYYY-MM-DD
 - fetch THEORICAL ENROLLMENT COUNT, instead of just LAST ENROLLMENT COUNT ( if you want to do predictive models and it sees a Phase 3 trial with only 5 patients it'll obviously knows it's a TERMINATED trial => data leakage )
 - location of the trial : for multi-centers trials, instead of assigning the first center's location in the list as the official country, we will take the location of the responsible party and use an external GoogleMapsGeocoding API to determine the country of the trial.
 - Fetch the name of the experimental product and eventual other placebo/active comparators
 
Scripts included in order of execution: 
1. CT_full_db.py : main script that generate / update the database
2. extra_infos_backendapi.py : add a new table with additional info from the record history page of each trials
3. async_Geoloc.py : gather responsible party's location ( requires a Geocoding googlemaps API key)
4. pin_point_the_experimental_drug.py + example_determination_molecules_interventionelles.ipynb : Fetch the name of the experimental product and eventual other placebo/active comparators + an example in a jupyter notebook
5. (depreciated) fetch the drug id from drugbank.co (need to update the code to use selenium instead of beautifulsoup)
