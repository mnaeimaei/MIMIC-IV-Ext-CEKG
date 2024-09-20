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
    Select * from `your-project-id1234.I01_Log.TabA1`  ;
    
    ALTER TABLE `your-project-id1234.I01_Log.Temp`  
    ADD column  c1 STRING ;
    
    UPDATE `your-project-id1234.I01_Log.Temp`  
    SET c1 ="A"
    WHERE c1 is null ;
    
    create table `your-project-id1234.I01_Log.TabB01`   as
    Select 
    Row_number() over (partition by c1 order by Timestamps, subject_id) as ID_unique, subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    from `your-project-id1234.I01_Log.Temp`  ;
    
    drop table `your-project-id1234.I01_Log.Temp`  ;

    
    
    
CREATE TABLE `your-project-id1234.I01_Log.TabB02`  AS
     SELECT distinct ID_unique, subject_id,   CAST(hadm_id AS STRING) AS hadm_id, Timestamps FROM `your-project-id1234.I01_Log.TabB01` 
order by Timestamps, subject_id   ;
        
        
        CREATE TABLE `your-project-id1234.I01_Log.TabB03`  AS
     SELECT distinct subject_id,   CAST(hadm_id AS STRING) AS hadm_id, Timestamps FROM `your-project-id1234.I01_Log.TabB02` 
order by Timestamps, subject_id   ;
        
    create table `your-project-id1234.I01_Log.Temp`   as
    Select * from `your-project-id1234.I01_Log.TabB03`  ;
    
    ALTER TABLE `your-project-id1234.I01_Log.Temp`  
    ADD column  c1 STRING ;
    
    UPDATE `your-project-id1234.I01_Log.Temp`  
    SET c1 ="A"
    WHERE c1 is null ;
    
    create table `your-project-id1234.I01_Log.TabB04`   as
    Select 
    Row_number() over (partition by c1 order by subject_id, Timestamps) as ID_Distinct, subject_id, hadm_id, Timestamps
    from `your-project-id1234.I01_Log.Temp`  ;
    
    drop table `your-project-id1234.I01_Log.Temp`  ;
    
    
CREATE TABLE  `your-project-id1234.I01_Log.TabB04_Rel`   AS
SELECT * FROM (
SELECT
a.ID_unique , b.ID_Distinct 
From `your-project-id1234.I01_Log.TabB02`   as a
LEFT JOIN `your-project-id1234.I01_Log.TabB04`   as b
ON
a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id AND a.Timestamps=b.Timestamps )    
; 

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
