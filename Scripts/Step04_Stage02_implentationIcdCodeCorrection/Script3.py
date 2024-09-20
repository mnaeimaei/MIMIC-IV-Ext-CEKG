import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



create table    `your-project-id1234.O_NonEvents_ICD6.icdCM3` as
select 
subject_id,
hadm_id,
seq_num,
long_title,
ICD10,
ICD10_Root,
ICD10_Root_title

from  `your-project-id1234.O_NonEvents_ICD6.icdCM2`  ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
