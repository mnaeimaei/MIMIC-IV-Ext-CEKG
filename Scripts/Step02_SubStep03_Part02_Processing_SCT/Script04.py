import os
from google.cloud import bigquery

symPath = '../gcKey/SNOMED_CT_Google_Cloud.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE  `snomed-ct-ml4219.K05_SCT.A_Definition_new`   AS
select distinct * from

(SELECT a.conceptId, a.term, b.MaxEffectiveTime from

(SELECT distinct conceptId,effectiveTime, term  
FROM `snomed-ct-ml4219.K04_SCT.Terminology_Text_Definition24` 
where active=1   
order by conceptId ) a

inner join (
SELECT distinct conceptId, MAX(effectiveTime) AS MaxEffectiveTime  
FROM `snomed-ct-ml4219.K04_SCT.Terminology_Text_Definition24` 
where active=1   
GROUP BY conceptId)b

ON a.conceptId = b.conceptId AND a.effectiveTime = b.MaxEffectiveTime);





CREATE TABLE `snomed-ct-ml4219.K05_SCT.A_Definition_new2`  AS

SELECT
a.conceptId, a.term,
b.RankN,

FROM `snomed-ct-ml4219.K05_SCT.A_Definition_new`  a
LEFT   JOIN (
SELECT  
conceptId, term,
Row_number() over (partition by  conceptId order by term asc) as RankN
FROM

(SELECT   DISTINCT conceptId, term  FROM `snomed-ct-ml4219.K05_SCT.A_Definition_new`  )  ) b
ON
a.conceptId = b.conceptId AND a.term = b.term;




CREATE TABLE `snomed-ct-ml4219.K05_SCT.A_Definition`  AS
(SELECT conceptId,term
 FROM `snomed-ct-ml4219.K05_SCT.A_Definition_new2`
where RankN=1);


drop TABLE `snomed-ct-ml4219.K05_SCT.A_Definition_new` ;
drop TABLE `snomed-ct-ml4219.K05_SCT.A_Definition_new2` ;
'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
