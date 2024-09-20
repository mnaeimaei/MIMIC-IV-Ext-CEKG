import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


###########################################################################

alter table `your-project-id1234.R_TimeA.Y_S` 
add column min DATETIME;

alter table `your-project-id1234.R_TimeA.Y_S` 
add column max DATETIME;


#######################################################


alter table `your-project-id1234.R_TimeA.Y_SH` 
add column min DATETIME;

alter table `your-project-id1234.R_TimeA.Y_SH` 
add column max DATETIME;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
