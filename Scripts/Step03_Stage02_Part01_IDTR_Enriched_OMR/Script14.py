import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE  `your-project-id1234.N03_OMR.OMR13`   AS
SELECT * FROM (
SELECT
a.subject_id , b.hadm_id , b.chartdate2 as Timestamps, a.seq_num , a.result_name , a.result_value,
From `your-project-id1234.N03_OMR.OMR`   as a
LEFT JOIN `your-project-id1234.N03_OMR.OMR12`   as b
ON
a.subject_id=b.subject_id AND a.chartdate=b.chartdate )    



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
