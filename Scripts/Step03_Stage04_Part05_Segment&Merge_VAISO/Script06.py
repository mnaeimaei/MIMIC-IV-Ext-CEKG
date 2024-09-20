import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

alter table   `your-project-id1234.N06_VASO.Tab82`   
add column dobutamine FLOAT64;


alter table   `your-project-id1234.N06_VASO.Tab82`   
add column dopamine FLOAT64;


alter table   `your-project-id1234.N06_VASO.Tab82`   
add column epinephrine FLOAT64;


alter table   `your-project-id1234.N06_VASO.Tab82`   
add column milrinone FLOAT64;


alter table   `your-project-id1234.N06_VASO.Tab82`   
add column norepinephrine FLOAT64;


alter table   `your-project-id1234.N06_VASO.Tab82`   
add column phenylephrine FLOAT64;


alter table   `your-project-id1234.N06_VASO.Tab82`   
add column vasopressin FLOAT64;

##########################################################################

CREATE TABLE  `your-project-id1234.N06_VASO.TabA2`  as 
SELECT stay_id , linkorderid , starttime , endtime , dobutamine , dobutamine_vaso_rate , dobutamine_vaso_amount , dopamine , dopamine_vaso_rate , dopamine_vaso_amount , epinephrine , epinephrine_vaso_rate , epinephrine_vaso_amount , milrinone , milrinone_vaso_rate , milrinone_vaso_amount , norepinephrine , norepinephrine_vaso_rate , norepinephrine_vaso_amount , phenylephrine , phenylephrine_vaso_rate , phenylephrine_vaso_amount , vasopressin , vasopressin_vaso_rate , vasopressin_vaso_amount 
FROM `your-project-id1234.N06_VASO.TabA1`  
union all
SELECT stay_id , linkorderid , starttime , endtime , dobutamine , dobutamine_vaso_rate , dobutamine_vaso_amount , dopamine , dopamine_vaso_rate , dopamine_vaso_amount , epinephrine , epinephrine_vaso_rate , epinephrine_vaso_amount , milrinone , milrinone_vaso_rate , milrinone_vaso_amount , norepinephrine , norepinephrine_vaso_rate , norepinephrine_vaso_amount , phenylephrine , phenylephrine_vaso_rate , phenylephrine_vaso_amount , vasopressin , vasopressin_vaso_rate , vasopressin_vaso_amount 
FROM `your-project-id1234.N06_VASO.Tab82`  ;
    


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
