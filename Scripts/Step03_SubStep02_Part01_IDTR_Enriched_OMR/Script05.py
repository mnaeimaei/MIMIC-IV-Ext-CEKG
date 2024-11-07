import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.N03_OMR.OMR04`  AS

SELECT
a.subject_id, a.hadm_id, a.chartdate, a.NewID, a.min, a.max, a.minDate, a.maxDate,
b.Dup
FROM `your-project-id1234.N03_OMR.OMR03`  a
LEFT   JOIN (
SELECT  
NewID, COUNT(*) dup
FROM `your-project-id1234.N03_OMR.OMR03` 
GROUP BY NewID) b
on 
a.NewID = b.NewID


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
