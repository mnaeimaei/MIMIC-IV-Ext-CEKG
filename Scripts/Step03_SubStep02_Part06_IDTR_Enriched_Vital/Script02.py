import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.N11_Vital.first_day_vitalsign_time`   AS
        SELECT distinct a.subject_id , a.stay_id , a.Timestamps , a.heart_rate_min , a.heart_rate_max , a.heart_rate_mean , a.sbp_min , a.sbp_max , a.sbp_mean , a.dbp_min , a.dbp_max , a.dbp_mean , a.mbp_min , a.mbp_max , a.mbp_mean , a.resp_rate_min , a.resp_rate_max , a.resp_rate_mean , a.temperature_min , a.temperature_max , a.temperature_mean , a.spo2_min , a.spo2_max , a.spo2_mean , a.glucose_min , a.glucose_max , a.glucose_mean FROM (
        SELECT
        a.subject_id , a.stay_id , a.heart_rate_min , a.heart_rate_max , a.heart_rate_mean , a.sbp_min , a.sbp_max , a.sbp_mean , a.dbp_min , a.dbp_max , a.dbp_mean , a.mbp_min , a.mbp_max , a.mbp_mean , a.resp_rate_min , a.resp_rate_max , a.resp_rate_mean , a.temperature_min , a.temperature_max , a.temperature_mean , a.spo2_min , a.spo2_max , a.spo2_mean , a.glucose_min , a.glucose_max , a.glucose_mean,
        b.careunit , b.min , b.max,
        DATETIME_ADD(min, INTERVAL 1 DAY) as Timestamps
        From `your-project-id1234.N11_Vital.first_day_vitalsign`   as a
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
