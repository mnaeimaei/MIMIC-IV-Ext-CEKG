import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE `your-project-id1234.I01_Log.TabD03A`  AS
SELECT 
subject_id, hadm_id, concat("Adm",RankN) hadm_id_syn
FROM `your-project-id1234.I01_Log.TabD02A` 
;


##################################


CREATE TABLE `your-project-id1234.I01_Log.TabD03B`  AS
SELECT 
subject_id, hadm_id, concat("Out",RankN) hadm_id_syn
FROM `your-project-id1234.I01_Log.TabD02B` 
;



        
        

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
