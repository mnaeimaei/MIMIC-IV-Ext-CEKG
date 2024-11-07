import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE  `your-project-id1234.N06_VASO.T_dobutamine`  as 
SELECT stay_id , linkorderid , vaso_rate dobutamine_vaso_rate , vaso_amount dobutamine_vaso_amount , starttime , endtime 
FROM `your-project-id1234.N06_VASO.dobutamine`  ;

CREATE TABLE  `your-project-id1234.N06_VASO.T_dopamine`  as 
SELECT stay_id , linkorderid , vaso_rate dopamine_vaso_rate , vaso_amount dopamine_vaso_amount , starttime , endtime 
FROM `your-project-id1234.N06_VASO.dopamine`  ;

CREATE TABLE  `your-project-id1234.N06_VASO.T_epinephrine`  as 
SELECT stay_id , linkorderid , vaso_rate epinephrine_vaso_rate , vaso_amount epinephrine_vaso_amount , starttime , endtime 
FROM `your-project-id1234.N06_VASO.epinephrine`  ;

CREATE TABLE  `your-project-id1234.N06_VASO.T_milrinone`  as 
SELECT stay_id , linkorderid , vaso_rate milrinone_vaso_rate , vaso_amount milrinone_vaso_amount , starttime , endtime 
FROM `your-project-id1234.N06_VASO.milrinone`  ;

CREATE TABLE  `your-project-id1234.N06_VASO.T_norepinephrine`  as 
SELECT stay_id , linkorderid , vaso_rate norepinephrine_vaso_rate , vaso_amount norepinephrine_vaso_amount , starttime , endtime 
FROM `your-project-id1234.N06_VASO.norepinephrine`  ;

CREATE TABLE  `your-project-id1234.N06_VASO.T_phenylephrine`  as 
SELECT stay_id , linkorderid , vaso_rate phenylephrine_vaso_rate , vaso_amount phenylephrine_vaso_amount , starttime , endtime 
FROM `your-project-id1234.N06_VASO.phenylephrine`  ;

CREATE TABLE  `your-project-id1234.N06_VASO.T_vasopressin`  as 
SELECT stay_id , linkorderid , vaso_rate vasopressin_vaso_rate , vaso_amount vasopressin_vaso_amount , starttime , endtime 
FROM `your-project-id1234.N06_VASO.vasopressin`  ;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
