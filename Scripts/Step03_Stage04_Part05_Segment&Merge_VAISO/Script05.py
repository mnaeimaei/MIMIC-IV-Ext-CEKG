import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE  `your-project-id1234.N06_VASO.TabA1`   AS
SELECT distinct * FROM (
SELECT
a.stay_id , b.linkorderid , a.starttime , a.endtime ,  

a.dobutamine ,  b.dobutamine_vaso_rate , b.dobutamine_vaso_amount , 
a.dopamine , b.dopamine_vaso_rate , b.dopamine_vaso_amount , 
a.epinephrine , b.epinephrine_vaso_rate , b.epinephrine_vaso_amount ,   
a.milrinone, b.milrinone_vaso_rate , b.milrinone_vaso_amount , 
a.norepinephrine , b.norepinephrine_vaso_rate , b.norepinephrine_vaso_amount ,  
a.phenylephrine , b.phenylephrine_vaso_rate , b.phenylephrine_vaso_amount , 
a.vasopressin , b.vasopressin_vaso_rate , b.vasopressin_vaso_amount

From `your-project-id1234.N06_VASO.Tab_agent`   as a
LEFT JOIN `your-project-id1234.N06_VASO.Tab81`   as b
ON
a.stay_id=b.stay_id AND a.starttime=b.starttime AND a.endtime=b.endtime )   ; 



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
