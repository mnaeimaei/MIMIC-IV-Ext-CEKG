import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



    CREATE TABLE  `your-project-id1234.N06_VASO.TabA4`  as 
    SELECT stay_id , linkorderid , starttime , endtime , 
    dobutamine_vaso_rate , 
    dobutamine_vaso_amount , 
    dopamine_vaso_rate , 
    dopamine_vaso_amount , 
    epinephrine_vaso_rate , 
    epinephrine_vaso_amount , 
    milrinone_vaso_rate , 
    milrinone_vaso_amount , 
    norepinephrine_vaso_rate , 
    norepinephrine_vaso_amount , 
    norepinephrine_equivalent_dose , 
    phenylephrine_vaso_rate , 
    phenylephrine_vaso_amount , 
    vasopressin_vaso_rate , 
    vasopressin_vaso_amount 
    FROM `your-project-id1234.N06_VASO.TabA3`  ;
    

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
