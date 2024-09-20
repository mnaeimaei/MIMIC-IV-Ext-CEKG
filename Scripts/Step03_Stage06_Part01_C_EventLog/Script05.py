import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


         
update `your-project-id1234.I01_Log.TabB05`  
set previous=null
where hadm_id="0" and previous<>"0";
        
        
         
update `your-project-id1234.I01_Log.TabB05`  
set previous="0"
where  previous is not null ;


         
update `your-project-id1234.I01_Log.TabB05`  
set previous="1"
where  previous is null ;

alter table `your-project-id1234.I01_Log.TabB05`  
add column num string;

update `your-project-id1234.I01_Log.TabB05`  
set num="1"
where  num is null ;


         

        
'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
