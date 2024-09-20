import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.R_TimeB.SHI_2`   AS
        SELECT * FROM (
        SELECT
        a.subject_id , a.hadm_id , a.stay_id , a.min , a.max,
        b.first_careunit , b.last_careunit
        From `your-project-id1234.R_TimeB.SHI`   as a
        LEFT JOIN `your-project-id1234.S_icuStays.1_icuStays`   as b
        ON
        a.subject_id=b.subject_id and a.hadm_id=b.hadm_id and a.stay_id=b.stay_id )    
    ; 
    
    
        CREATE TABLE  `your-project-id1234.R_TimeB.SI_2`   AS
        SELECT * FROM (
        SELECT
        a.subject_id , a.stay_id , a.min , a.max,
        b.first_careunit , b.last_careunit
        From `your-project-id1234.R_TimeB.SI`   as a
        LEFT JOIN `your-project-id1234.S_icuStays.1_icuStays`   as b
        ON
        a.subject_id=b.subject_id and a.stay_id=b.stay_id )    
    ; 
    
    
        
        CREATE TABLE  `your-project-id1234.R_TimeB.I_2`   AS
        SELECT * FROM (
        SELECT
        a.stay_id , a.min , a.max,
        b.first_careunit , b.last_careunit
        From `your-project-id1234.R_TimeB.I`   as a
        LEFT JOIN `your-project-id1234.S_icuStays.1_icuStays`   as b
        ON
        a.stay_id=b.stay_id )    
    ; 
    
    
    
        CREATE TABLE  `your-project-id1234.R_TimeB.HI_2`   AS
        SELECT * FROM (
        SELECT
        a.hadm_id , a.stay_id , a.min , a.max,
        b.first_careunit , b.last_careunit
        From `your-project-id1234.R_TimeB.SHI`   as a
        LEFT JOIN `your-project-id1234.S_icuStays.1_icuStays`   as b
        ON
        a.hadm_id=b.hadm_id and a.stay_id=b.stay_id )    
    ; 
    
    
    


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
