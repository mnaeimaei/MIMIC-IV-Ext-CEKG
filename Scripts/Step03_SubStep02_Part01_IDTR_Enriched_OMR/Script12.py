import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.N03_OMR.OMR111`  AS
SELECT subject_id, hadm_id, chartdate,  chartdate2
FROM `your-project-id1234.N03_OMR.OMR101` ;

CREATE TABLE `your-project-id1234.N03_OMR.OMR112`  AS
SELECT subject_id, hadm_id, chartdate, chartdate2
FROM `your-project-id1234.N03_OMR.OMR102` ;


 

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
