import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create table `your-project-id1234.N04_CHB.a_Lab` as
SELECT 
subject_id	,hadm_id,labevent_id,specimen_id,itemid	,order_provider_id,charttime,storetime,value,valuenum,valueuom,ref_range_lower,ref_range_upper,flag	,priority,comments,label,fluid,category
FROM `your-project-id1234.N04_CHB.A2` 

union all
SELECT
subject_id	,hadm_id,labevent_id,specimen_id,itemid	,order_provider_id,charttime,storetime,value,valuenum,valueuom,ref_range_lower,ref_range_upper,flag	,priority,comments,label,fluid,category
FROM `your-project-id1234.N04_CHB.A1` 


union all
SELECT
subject_id	,hadm_id,labevent_id,specimen_id,itemid	,order_provider_id,charttime,storetime,value,valuenum,valueuom,ref_range_lower,ref_range_upper,flag	,priority,comments,label,fluid,category
FROM `your-project-id1234.N04_CHB.A3` 

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
