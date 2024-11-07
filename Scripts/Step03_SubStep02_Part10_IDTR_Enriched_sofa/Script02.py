import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.N16_sofa.first_day_sofa_time`   AS
        SELECT distinct a.subject_id , a.hadm_id , a.stay_id , a.Timestamps, a.SOFA , a.respiration , a.coagulation , a.liver , a.cardiovascular , a.cns , a.renal FROM (
        SELECT
        a.subject_id , a.hadm_id , a.stay_id , a.SOFA , a.respiration , a.coagulation , a.liver , a.cardiovascular , a.cns , a.renal,
        b.careunit , b.min , b.max,
        DATETIME_ADD(min, INTERVAL 1 DAY) as Timestamps
        From `your-project-id1234.N16_sofa.first_day_sofa`   as a
        LEFT JOIN `your-project-id1234.R_TimeC.SHI`   as b
        ON
        a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id AND a.stay_id=b.stay_id 


        )  a  ;



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
