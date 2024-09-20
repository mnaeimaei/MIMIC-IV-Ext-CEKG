import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM13_aggrDis`  AS
        SELECT hadm_id, STRING_AGG(Disorder_ID,"," ORDER BY Disorder_ID)  as Disorder_ID_agg
        FROM `your-project-id1234.O_NonEvents_ICD3.icdCM12`  
        GROUP BY hadm_id;
        
        
        CREATE TABLE  `your-project-id1234.O_NonEvents_ICD3.icdCM12_int`   AS
        SELECT distinct hadm_id,   Disorder_ID FROM `your-project-id1234.O_NonEvents_ICD3.icdCM08` ;




'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
