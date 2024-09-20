import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



CREATE TABLE  `your-project-id1234.I17_DK7.TabA_class_mainEntities`   AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID  , b.Disorders, a.subject_id , a.hadm_id 
From  `your-project-id1234.I01_Log.TabC01`   as a
LEFT JOIN `your-project-id1234.O_NonEvents_ICD3.icdCM08_class`   as b
ON
a.hadm_id=b.hadm_id )      
where Disorders is not null   and   Activity_Value_ID is not null    ; 

        
        

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
