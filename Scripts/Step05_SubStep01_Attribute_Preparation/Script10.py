import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.O_NonEvents_ICD3.icdCM12`   AS
        SELECT distinct hadm_id,   CAST(Disorder_ID AS STRING) AS Disorder_ID FROM `your-project-id1234.O_NonEvents_ICD3.icdCM08` ;




'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
