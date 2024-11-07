import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

    create table `your-project-id1234.I01_Log.Temp`   as
    Select * from `your-project-id1234.I01_Log.TabB14`  ;
    
    ALTER TABLE `your-project-id1234.I01_Log.Temp`  
    ADD column  c1 STRING ;
    
    UPDATE `your-project-id1234.I01_Log.Temp`  
    SET c1 ="A"
    WHERE c1 is null ;
    
    create table `your-project-id1234.I01_Log.TabB15`   as
    Select 
    Row_number() over (partition by c1 order by subject_id, Timestamps) as ID_Distinct_N, subject_id, hadm_id, Timestamps, helper
    from `your-project-id1234.I01_Log.Temp`  ;
    
    drop table `your-project-id1234.I01_Log.Temp`  ;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
