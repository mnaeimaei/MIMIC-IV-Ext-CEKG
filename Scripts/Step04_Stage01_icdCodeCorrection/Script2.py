import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)



os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create table `mimic-four-ml4219.O_NonEvents_ICD7.icdCM1_short` as
SELECT 
 
 icd_code, icd_version, long_title
FROM `mimic-four-ml4219.S_icd_diagnoses.1_icdCM`; 


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
