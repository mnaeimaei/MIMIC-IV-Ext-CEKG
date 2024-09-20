import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


    CREATE TABLE  `your-project-id1234.N05_KDIGO.TabC2`  as 
    SELECT subject_id , hadm_id , stay_id , charttime , creat , creat_low_past_48hr , creat_low_past_7day , aki_stage_creat ,  aki_stage_uo , aki_stage_crrt , aki_stage , aki_stage_smoothed , weight , urineoutput_6hr , urineoutput_12hr , urineoutput_24hr ,uo_rt_6hr ,  uo_rt_12hr , uo_rt_24hr , uo_tm_6hr , uo_tm_12hr , uo_tm_24hr 
    FROM `your-project-id1234.N05_KDIGO.TabC1`  ;
    

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
