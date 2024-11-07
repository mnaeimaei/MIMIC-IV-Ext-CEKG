import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE  `your-project-id1234.N03_OMR.OMR03`   AS
SELECT distinct * FROM (
SELECT
a.subject_id ,  b. hadm_id, a.chartdate ,  a.NewID,
b.min, b.max, b.minDate, b.maxDate
From `your-project-id1234.N03_OMR.OMR02`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SH`   as b
ON
a.subject_id=b.subject_id 
AND  a.chartdate>=b.minDate AND a.chartdate<=b.maxDate 
)    



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
