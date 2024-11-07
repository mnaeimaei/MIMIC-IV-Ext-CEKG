import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.N03_OMR.OMR101`  AS
SELECT subject_id, hadm_id, chartdate, min, max, CAST(CAST(chartdate AS TIMESTAMP) AS DATETIME) as chartdate2
 from  `your-project-id1234.N03_OMR.OMR091`;

#################################################################3

CREATE TABLE `your-project-id1234.N03_OMR.OMR102`  AS
select * from 
(SELECT 
subject_id, hadm_id, chartdate, min, max, 
TIMESTAMP_ADD(CAST(CAST(chartdate AS TIMESTAMP) AS DATETIME), INTERVAL 12 HOUR ) AS chartdate2
FROM `your-project-id1234.N03_OMR.OMR092` );

alter table  `your-project-id1234.N03_OMR.OMR102`  
add column ddd int64;

update  `your-project-id1234.N03_OMR.OMR102`  
set ddd=1
where chartdate2>=min and 
 chartdate2<=max  ;
 
 
update  `your-project-id1234.N03_OMR.OMR102`  
SET chartdate2 = TIMESTAMP_ADD(min, INTERVAL CAST(TIMESTAMP_DIFF(max, min, SECOND) / 2 AS INT64) SECOND)
where ddd is null;



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
