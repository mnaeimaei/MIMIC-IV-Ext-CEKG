import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.N03_OMR.OMR061`  AS
SELECT
a.subject_id, a.chartdate, a.NewID, a.hadm_id,
b.RankN,
a.min, a.max, a.minDate, a.maxDate, a.Dup
FROM `your-project-id1234.N03_OMR.OMR051`  a
LEFT   JOIN (
SELECT  
subject_id, chartdate, NewID, hadm_id,
Row_number() over (partition by   subject_id, chartdate, NewID order by hadm_id asc) as RankN
FROM
(SELECT   DISTINCT subject_id, chartdate, NewID, hadm_id  FROM `your-project-id1234.N03_OMR.OMR051`  )  ) b
ON
a.subject_id = b.subject_id AND a.chartdate = b.chartdate AND a.NewID = b.NewID AND a.hadm_id = b.hadm_id;

#####################################

CREATE TABLE `your-project-id1234.N03_OMR.OMR062`  AS
SELECT * FROM `your-project-id1234.N03_OMR.OMR052` ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
