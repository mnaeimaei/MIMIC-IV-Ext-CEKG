import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



        
        
        
CREATE TABLE  `your-project-id1234.I17_DK7.TabA_class`   AS
SELECT distinct Activity_Value_ID  , Disorders
From  `your-project-id1234.I17_DK7.TabA_class_mainEntities`      ;

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
