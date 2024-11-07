import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.O_NonEvents_ICD6.S1`   AS
        SELECT * FROM (
        SELECT
        a.subject_id , a.hadm_id , a.seq_num , a.long_title , a.ICD10 , a.ICD10_Root , a.ICD10_Root_title,
        b.Dup as  MC_num
        From `your-project-id1234.O_NonEvents_ICD6.icdCM3`   as a
        LEFT JOIN `your-project-id1234.O_NonEvents_ICD6.T1`   as b
        ON
        a.hadm_id=b.hadm_id )     ; 
        
        
        CREATE TABLE  `your-project-id1234.O_NonEvents_ICD6.icdCM4`   AS
        SELECT * FROM (
        SELECT
        a.subject_id , a.hadm_id , a.seq_num , a.long_title , a.ICD10 , a.ICD10_Root , a.ICD10_Root_title, a.MC_num,
        b.Dup as  hadm_num
        From `your-project-id1234.O_NonEvents_ICD6.S1`   as a
        LEFT JOIN `your-project-id1234.O_NonEvents_ICD6.T2`   as b
        ON
        a.subject_id=b.subject_id )     ; 

drop table  `your-project-id1234.O_NonEvents_ICD6.T1` ;
drop table  `your-project-id1234.O_NonEvents_ICD6.T2` ;
drop table  `your-project-id1234.O_NonEvents_ICD6.S1` ;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
