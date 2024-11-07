import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.R_TimeB.ST_2`   AS
        SELECT * FROM (
        SELECT
        a.subject_id , a.transfer_id , a.min , a.max,
        b.eventtype , b.careunit 
        From `your-project-id1234.R_TimeB.ST`   as a
        LEFT JOIN `your-project-id1234.S_Transfers.1_Transfer`   as b
        ON
        a.transfer_id=b.transfer_id )    
    ; 
    
    
    
        CREATE TABLE  `your-project-id1234.R_TimeB.T_2`   AS
        SELECT * FROM (
        SELECT
        a.transfer_id , a.min , a.max,
        b.eventtype , b.careunit 
        From `your-project-id1234.R_TimeB.T`   as a
        LEFT JOIN `your-project-id1234.S_Transfers.1_Transfer`   as b
        ON
        a.transfer_id=b.transfer_id )    
    ; 
    
    
    
        CREATE TABLE  `your-project-id1234.R_TimeB.HT_2`   AS
        SELECT * FROM (
        SELECT
        a.hadm_id , a.transfer_id , a.min , a.max,
        b.eventtype , b.careunit 
        From `your-project-id1234.R_TimeB.HT`   as a
        LEFT JOIN `your-project-id1234.S_Transfers.1_Transfer`   as b
        ON
        a.transfer_id=b.transfer_id )    
    ; 
    
    
        CREATE TABLE  `your-project-id1234.R_TimeB.SHT_2`   AS
        SELECT * FROM (
        SELECT
        a.subject_id , a.hadm_id, a.transfer_id , a.min , a.max,
        b.eventtype , b.careunit
        From `your-project-id1234.R_TimeB.SHT`   as a
        LEFT JOIN `your-project-id1234.S_Transfers.1_Transfer`   as b
        ON
        a.transfer_id=b.transfer_id  )    
    ; 

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
