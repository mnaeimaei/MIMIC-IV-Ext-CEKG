import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create table  `your-project-id1234.N04_CHB.d_Lab`  as

SELECT distinct
subject_id, hadm_id, storetime as Timestp,
new_fluid as fluid, 
new_Label as fluidLabel, 
short_Label as fluidLabelShort, 
value, 
CAST (valuenum AS STRING) AS fluidLabelValue, 
new_valueuom as fluidLabelUM,
Normal_Range as fluidLabelRange,
flag as fluidLabelFlag
FROM `your-project-id1234.N04_CHB.c_Lab` 



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
