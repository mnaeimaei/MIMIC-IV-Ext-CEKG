import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create table `your-project-id1234.N06_VASO.Tab1` as
(SELECT stay_id, linkorderid, starttime, endtime FROM `your-project-id1234.N06_VASO.dobutamine` 
union all
SELECT stay_id, linkorderid, starttime, endtime FROM `your-project-id1234.N06_VASO.dopamine` 
union all
SELECT stay_id, linkorderid, starttime, endtime FROM `your-project-id1234.N06_VASO.epinephrine` 
union all
SELECT stay_id, linkorderid, starttime, endtime FROM `your-project-id1234.N06_VASO.milrinone` 
union all
SELECT stay_id, linkorderid, starttime, endtime FROM `your-project-id1234.N06_VASO.norepinephrine` 
union all
SELECT stay_id, linkorderid, starttime, endtime FROM `your-project-id1234.N06_VASO.phenylephrine` 
union all
SELECT stay_id, linkorderid, starttime, endtime FROM `your-project-id1234.N06_VASO.vasopressin` );




'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
