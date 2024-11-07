import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE `your-project-id1234.N16_sofa.X_antibioticC1`  AS
SELECT 
subject_id, stay_id, hadm_id, Timestamps, ab_id, antibiotic
FROM `your-project-id1234.N16_sofa.X_antibioticA4` 
union distinct
SELECT 
subject_id, stay_id, hadm_id, Timestamps, ab_id, antibiotic
FROM `your-project-id1234.N16_sofa.X_antibioticB1` ;

#################################################################################################

CREATE TABLE `your-project-id1234.N16_sofa.X_cultureC1`  AS
SELECT 
subject_id, stay_id, hadm_id, Timestamps, specimen, positive_culture
FROM `your-project-id1234.N16_sofa.X_cultureA4` 
union distinct
SELECT 
subject_id, stay_id, hadm_id, Timestamps, specimen, positive_culture
FROM `your-project-id1234.N16_sofa.X_cultureB1` ;
#################################################################################################

CREATE TABLE `your-project-id1234.N16_sofa.X_suspected_infectionC1`  AS
SELECT 
subject_id, stay_id, hadm_id, Timestamps, suspected_infection
FROM `your-project-id1234.N16_sofa.X_suspected_infectionA4` 
union distinct
SELECT 
subject_id, stay_id, hadm_id, Timestamps, suspected_infection
FROM `your-project-id1234.N16_sofa.X_suspected_infectionB1` ;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
