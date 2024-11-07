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
    Select * from `your-project-id1234.I01_Log.TabE01`  ;

    ALTER TABLE `your-project-id1234.I01_Log.Temp`  
    ADD column  c1 STRING ;

    UPDATE `your-project-id1234.I01_Log.Temp`  
    SET c1 ="A"
    WHERE c1 is null ;

    create table `your-project-id1234.I01_Log.Temp2`   as
    Select 
    Row_number() over (partition by c1 order by subject_id, hadm_id) as NewID, subject_id, hadm_id, hadm_id_syn, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    from `your-project-id1234.I01_Log.Temp`  ;

    ALTER TABLE `your-project-id1234.I01_Log.Temp2`  
    ADD column  eventID STRING ;

    UPDATE `your-project-id1234.I01_Log.Temp2`  
    SET eventID = concat("e",NewID)
    WHERE eventID is null ;

    create table `your-project-id1234.I01_Log.TabE02`   as
    Select
    eventID, subject_id, hadm_id, hadm_id_syn, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    from `your-project-id1234.I01_Log.Temp2`  ;

    drop table `your-project-id1234.I01_Log.Temp`  ;
    drop table `your-project-id1234.I01_Log.Temp2`  ;

#######################################










'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
