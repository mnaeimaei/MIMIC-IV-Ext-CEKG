import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE  `your-project-id1234.N06_VASO.TabA3`   AS
SELECT * FROM (
SELECT
a.stay_id , a.linkorderid , a.starttime , a.endtime , a.dobutamine , a.dobutamine_vaso_rate , a.dobutamine_vaso_amount , a.dopamine , a.dopamine_vaso_rate , a.dopamine_vaso_amount , a.epinephrine , a.epinephrine_vaso_rate , a.epinephrine_vaso_amount , a.milrinone , a.milrinone_vaso_rate , a.milrinone_vaso_amount , a.norepinephrine , a.norepinephrine_vaso_rate , a.norepinephrine_vaso_amount , b.norepinephrine_equivalent_dose, a.phenylephrine , a.phenylephrine_vaso_rate , a.phenylephrine_vaso_amount , a.vasopressin , a.vasopressin_vaso_rate , a.vasopressin_vaso_amount,
From `your-project-id1234.N06_VASO.TabA2`   as a
LEFT JOIN `your-project-id1234.N06_VASO.norepinephrine_equivalent_dose`   as b
ON
a.stay_id=b.stay_id AND a.starttime=b.starttime AND a.endtime=b.endtime )    
; 



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
