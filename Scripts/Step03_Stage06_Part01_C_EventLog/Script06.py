import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        CREATE TABLE `your-project-id1234.I01_Log.TabB06_A`  AS
        SELECT * FROM `your-project-id1234.I01_Log.TabB05`
        where previous ="1";
        
        
        CREATE TABLE `your-project-id1234.I01_Log.TabB06_B`  AS
        SELECT * FROM `your-project-id1234.I01_Log.TabB05`
        where previous ="0";
        
        
        
        

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
