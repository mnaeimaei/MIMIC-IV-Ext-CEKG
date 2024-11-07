import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.N05_KDIGO.TabB1`   AS
        SELECT * FROM (
        SELECT
        b.subject_id , a.hadm_id , a.stay_id , a.charttime , 
        a.creat , 
        b.creat as creat2,
        a.creat_low_past_48hr , 
        b.creat_low_past_48hr as creat_low_past_48hr2 , 
        b.creat_low_past_7day as creat_low_past_7day2,
        a.creat_low_past_7day,  
        b.aki_stage_creat , b.uo_rt_6hr , b.uo_rt_12hr , b.uo_rt_24hr , b.aki_stage_uo , b.aki_stage_crrt , b.aki_stage , b.aki_stage_smoothed
        From `your-project-id1234.N05_KDIGO.TabA1`   as a
        LEFT JOIN `your-project-id1234.N05_KDIGO.TabA2`   as b
        ON
        a.hadm_id=b.hadm_id AND a.stay_id=b.stay_id AND a.charttime=b.charttime )        ; 


#####################################################################################

    CREATE TABLE  `your-project-id1234.N05_KDIGO.TabB2`  as 
    SELECT subject_id , hadm_id , stay_id , charttime , creat ,  creat_low_past_48hr ,  creat_low_past_7day , aki_stage_creat , uo_rt_6hr , uo_rt_12hr , uo_rt_24hr , aki_stage_uo , aki_stage_crrt , aki_stage , aki_stage_smoothed 
    FROM `your-project-id1234.N05_KDIGO.TabB1`  ;
    
    
    #####################################################################################

    CREATE TABLE  `your-project-id1234.N05_KDIGO.TabB3`  as 
    SELECT *
    FROM `your-project-id1234.N05_KDIGO.TabA3`  ;
    

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
