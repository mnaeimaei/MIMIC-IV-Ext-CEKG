import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.I02_otherEntities.TabA_Age`  AS
SELECT distinct
Origin, ID, Name, Value, Category, subject_id, hadm_id
FROM `your-project-id1234.O_NonEvents_ClusData.Q01_ageX3` ;

CREATE TABLE `your-project-id1234.I02_otherEntities.TabA_Gender`  AS
SELECT distinct
Origin, ID, Name, Value, Category, subject_id, hadm_id
FROM `your-project-id1234.O_NonEvents_ClusData.Q01_PatientHadmGender` ;


CREATE TABLE `your-project-id1234.I02_otherEntities.TabA_Others`  AS
SELECT  distinct 
Origin, ID, Name, Value, Category, subject_id, hadm_id
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM17_other_ent_final`;

 

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
