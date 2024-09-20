import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE  `your-project-id1234.N16_sofa.sepsis3_new`   AS
SELECT distinct
subject_id, stay_id, sofa_time as Timestamps, sofa_score, respiration, coagulation, liver, cardiovascular, cns, renal, sepsis3
FROM `your-project-id1234.N16_sofa.sepsis3` 
where sofa_time is not null;




CREATE TABLE  `your-project-id1234.N16_sofa.sepsis3_new2`   AS
SELECT distinct * FROM (
SELECT
a.subject_id , b.hadm_id, a.stay_id , a.Timestamps , a.sofa_score , a.respiration , a.coagulation , a.liver , a.cardiovascular , a.cns , a.renal , a.sepsis3,
From `your-project-id1234.N16_sofa.sepsis3_new`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SHI`   as b
ON
a.subject_id=b.subject_id AND a.stay_id=b.stay_id         )        ; 

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
