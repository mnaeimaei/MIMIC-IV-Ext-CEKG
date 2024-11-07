import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create table `your-project-id1234.I12_DK3.TabA` as
SELECT 
b.ID as Disorders,
a.icd_code,
from `your-project-id1234.O_NonEvents_ICD3.icdCM21_DK3` a
left join `your-project-id1234.I02_otherEntities.TabC` b
on a.Disorders=b.Name;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
