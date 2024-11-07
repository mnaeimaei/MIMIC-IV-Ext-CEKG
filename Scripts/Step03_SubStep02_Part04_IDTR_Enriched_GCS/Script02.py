import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



        CREATE TABLE  `your-project-id1234.N09_GCS.first_day_gcs_time`   AS
        SELECT distinct a.subject_id , a.stay_id , a.Timestamps , a.gcs_min , a.gcs_motor , a.gcs_verbal , a.gcs_eyes , a.gcs_unable FROM (
        SELECT
        a.subject_id , a.stay_id , a.gcs_min , a.gcs_motor , a.gcs_verbal , a.gcs_eyes , a.gcs_unable,
        b.careunit , b.min , b.max,
        DATETIME_ADD(min, INTERVAL 1 DAY) as Timestamps
        From `your-project-id1234.N09_GCS.first_day_gcs`   as a
        LEFT JOIN `your-project-id1234.R_TimeC.SI`   as b
        ON
        a.subject_id=b.subject_id AND a.stay_id=b.stay_id 


        )  a  




'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
