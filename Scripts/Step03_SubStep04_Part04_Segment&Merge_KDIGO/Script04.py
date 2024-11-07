import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



        CREATE TABLE  `your-project-id1234.N05_KDIGO.TabC1`   AS
        SELECT * FROM (
        SELECT
        a.subject_id , 
        a.hadm_id , 
        a.stay_id , 
        a.charttime , 
        a.creat , 
        a.creat_low_past_48hr , 
        a.creat_low_past_7day , 
        a.aki_stage_creat , 
        a.uo_rt_6hr , 
                b.uo_rt_6hr as uo_rt_6hr2, 
        a.uo_rt_12hr , 
                b.uo_rt_12hr as uo_rt_12hr2, 
        a.uo_rt_24hr , 
                b.uo_rt_24hr as uo_rt_24hr2, 
        a.aki_stage_uo , 
        a.aki_stage_crrt , 
        a.aki_stage , 
        a.aki_stage_smoothed,
        
        b.weight , 
        b.urineoutput_6hr , 
        b.urineoutput_12hr , 
        b.urineoutput_24hr ,
        b.uo_tm_6hr ,
        b.uo_tm_12hr , 
        b.uo_tm_24hr,
        From `your-project-id1234.N05_KDIGO.TabB2`   as a
        LEFT JOIN `your-project-id1234.N05_KDIGO.TabB3`   as b
        ON
        a.stay_id=b.stay_id AND a.charttime=b.charttime )    
    ; 


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
