import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        
CREATE TABLE `your-project-id1234.I01_Log.TabB12`  AS
SELECT distinct ID_Distinct	,subject_id, hadm_id, Timestamps  From `your-project-id1234.I01_Log.TabB11`;
   
ALTER TABLE `your-project-id1234.I01_Log.TabB12`  
ADD column  helper STRING ;

UPDATE `your-project-id1234.I01_Log.TabB12`  
SET helper ="1"
where hadm_id  not LIKE "O%";

UPDATE `your-project-id1234.I01_Log.TabB12`  
SET helper ="0"
where hadm_id  LIKE "O%";
    
'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
