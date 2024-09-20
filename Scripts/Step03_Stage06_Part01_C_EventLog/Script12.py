import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

           
CREATE TABLE `your-project-id1234.I01_Log.TabB11`  AS
SELECT distinct *  From `your-project-id1234.I01_Log.TabB10`;



UPDATE `your-project-id1234.I01_Log.TabB11`  
SET hadm_id =hadm_N
WHERE hadm_id="0" ;

            
             
        

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
