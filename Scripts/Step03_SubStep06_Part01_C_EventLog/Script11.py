import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query until these query was satisfied
#SELECT * FROM `your-project-id1234.I01_Log.TabB10`
#where hadm_N="0" and hadm_id="0"


query1 = f'''            


        
CREATE TABLE `your-project-id1234.I01_Log.TabTemp`  AS
SELECT * FROM (
SELECT
a.ID_Distinct , a.subject_id , a.hadm_id , a.Timestamps, a.zeroFinder, a.hadm_N,
b.hadm_id as H_prev, b.zeroFinder as Z_prev, b.hadm_N as N_prev
From `your-project-id1234.I01_Log.TabB10`   as a
LEFT JOIN `your-project-id1234.I01_Log.TabB10`   as b
ON
a.ID_Distinct=(b.ID_Distinct)+1 and a.subject_id=b.subject_id)    ;


update  `your-project-id1234.I01_Log.TabTemp`  
set hadm_N=N_prev, zeroFinder=Z_prev
where hadm_id="0" and H_prev="0" and Z_prev="1";

drop Table  `your-project-id1234.I01_Log.TabB10`  ;


CREATE TABLE `your-project-id1234.I01_Log.TabB10`  AS
SELECT distinct ID_Distinct,subject_id,hadm_id, Timestamps, zeroFinder,  hadm_N   From `your-project-id1234.I01_Log.TabTemp`;

drop Table  `your-project-id1234.I01_Log.TabTemp`  ;  


        

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
