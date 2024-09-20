import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


update `your-project-id1234.I01_Log.TabE03` 
set En2_ID=null
where    En2_ID LIKE 'O%';

update `your-project-id1234.I01_Log.TabE03` 
set Ent2=null
where    En2_ID is null ;


################################################


update `your-project-id1234.I01_Log.TabE03` 
set En3_ID=null
where    En3_ID not LIKE 'O%';

update `your-project-id1234.I01_Log.TabE03` 
set Ent3=null
where    En3_ID is null ;





################################################



update `your-project-id1234.I01_Log.TabE03` 
set En4_ID=null
where    En4_ID not LIKE 'Adm%';

update `your-project-id1234.I01_Log.TabE03` 
set Ent4=null
where    En4_ID is null ;



################################################

update `your-project-id1234.I01_Log.TabE03` 
set En5_ID=null
where    En5_ID not LIKE 'Out%';

update `your-project-id1234.I01_Log.TabE03` 
set Ent5=null
where    En5_ID is null ;


################################################



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
