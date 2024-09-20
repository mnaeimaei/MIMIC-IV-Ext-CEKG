import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.N06_VASO.Tab81`   AS
        SELECT distinct 
        stay_id , linkorderid , starttime , endtime , dobutamine_vaso_rate , dobutamine_vaso_amount , dopamine_vaso_rate , dopamine_vaso_amount , epinephrine_vaso_rate , epinephrine_vaso_amount , milrinone_vaso_rate , milrinone_vaso_amount , norepinephrine_vaso_rate , norepinephrine_vaso_amount , phenylephrine_vaso_rate , phenylephrine_vaso_amount , vasopressin_vaso_rate , vasopressin_vaso_amount 
        FROM (
        SELECT
        a.stay_id , a.linkorderid , a.starttime , a.endtime , a.dobutamine_vaso_rate , a.dobutamine_vaso_amount , a.dopamine_vaso_rate , a.dopamine_vaso_amount , a.epinephrine_vaso_rate , a.epinephrine_vaso_amount , a.milrinone_vaso_rate , a.milrinone_vaso_amount , a.norepinephrine_vaso_rate , a.norepinephrine_vaso_amount , a.phenylephrine_vaso_rate , a.phenylephrine_vaso_amount , a.vasopressin_vaso_rate , a.vasopressin_vaso_amount,
        b.stay_id ss
        From `your-project-id1234.N06_VASO.Tab8`   as a
        LEFT JOIN `your-project-id1234.N06_VASO.Tab_agent`   as b
        ON
        a.stay_id=b.stay_id AND a.starttime=b.starttime AND a.endtime=b.endtime )    
        where   ss is not null    ; 


        CREATE TABLE  `your-project-id1234.N06_VASO.Tab82`   AS
        SELECT distinct 
        stay_id , linkorderid , starttime , endtime , dobutamine_vaso_rate , dobutamine_vaso_amount , dopamine_vaso_rate , dopamine_vaso_amount , epinephrine_vaso_rate , epinephrine_vaso_amount , milrinone_vaso_rate , milrinone_vaso_amount , norepinephrine_vaso_rate , norepinephrine_vaso_amount , phenylephrine_vaso_rate , phenylephrine_vaso_amount , vasopressin_vaso_rate , vasopressin_vaso_amount 
        FROM (
        SELECT
        a.stay_id , a.linkorderid , a.starttime , a.endtime , a.dobutamine_vaso_rate , a.dobutamine_vaso_amount , a.dopamine_vaso_rate , a.dopamine_vaso_amount , a.epinephrine_vaso_rate , a.epinephrine_vaso_amount , a.milrinone_vaso_rate , a.milrinone_vaso_amount , a.norepinephrine_vaso_rate , a.norepinephrine_vaso_amount , a.phenylephrine_vaso_rate , a.phenylephrine_vaso_amount , a.vasopressin_vaso_rate , a.vasopressin_vaso_amount,
        b.stay_id ss
        From `your-project-id1234.N06_VASO.Tab8`   as a
        LEFT JOIN `your-project-id1234.N06_VASO.Tab_agent`   as b
        ON
        a.stay_id=b.stay_id AND a.starttime=b.starttime AND a.endtime=b.endtime )    
        where   ss is null    ; 



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
