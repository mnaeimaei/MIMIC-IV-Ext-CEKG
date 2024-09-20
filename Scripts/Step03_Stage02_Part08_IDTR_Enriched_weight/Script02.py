import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        CREATE TABLE  `your-project-id1234.N13_weight.first_day_weight_time`   AS
        SELECT distinct a.subject_id , a.stay_id , a.min  as starttime,a.endtime, a.weight_admit , a.weight , a.weight_min , a.weight_max FROM (
        SELECT
        a.subject_id , a.stay_id , a.weight_admit , a.weight , a.weight_min , a.weight_max,
        b.careunit , b.min , b.max, 
        DATETIME_ADD(min, INTERVAL 1 DAY) as endtime
        From `your-project-id1234.N13_weight.first_day_weight`   as a
        LEFT JOIN `your-project-id1234.R_TimeC.SI`   as b
        ON
        a.subject_id=b.subject_id AND a.stay_id=b.stay_id 
        )  a  ;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
