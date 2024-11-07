import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            
CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM20_ICD`  AS
SELECT distinct
concat("ICD") ClinicalEntity,
ICD10_Root icd_code, 
concat(10) icd_version, 
ICD10_Root_title icd_code_title
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM06` 



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
