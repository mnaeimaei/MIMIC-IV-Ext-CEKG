import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create schema `your-project-id1234.O_NonEvents_ClusData` ;

create table `your-project-id1234.O_NonEvents_ClusData.Q01_Patient` as
SELECT * FROM `your-project-id1234.S_Patients.1_Patient` ;



create table `your-project-id1234.O_NonEvents_ClusData.Q01_age1` as
SELECT subject_id,hadm_id, anchor_age, anchor_year, age FROM `your-project-id1234.S_Derived14.age` ;

 create table `your-project-id1234.O_NonEvents_ClusData.Q01_age2` as
 
 SELECT     
    a.subject_id
    , a.hadm_id
    , a.admittime
    , b.anchor_age
    , b.anchor_year
    , DATETIME_DIFF(a.admittime, DATETIME(b.anchor_year, 1, 1, 0, 0, 0), YEAR) + b.anchor_age AS age
FROM `your-project-id1234.S_Admission.2_Admission` a
INNER JOIN `your-project-id1234.S_Patients.1_Patient` b
ON a.subject_id = b.subject_id;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
