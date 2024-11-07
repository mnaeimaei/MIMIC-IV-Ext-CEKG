import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM08_class`  AS
SELECT distinct   CAST(hadm_id AS STRING) AS hadm_id,  Disorder_Origin as Disorders
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM08` ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
