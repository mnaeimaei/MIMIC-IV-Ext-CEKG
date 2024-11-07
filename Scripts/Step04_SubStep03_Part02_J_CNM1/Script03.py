import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

alter table `your-project-id1234.O_NonEvents_ICD3.icdCM07` 
add column num INT64;

update  `your-project-id1234.O_NonEvents_ICD3.icdCM07` 
set num = 1
where num is null;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
