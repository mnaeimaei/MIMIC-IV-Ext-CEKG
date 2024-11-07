import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

#### Create medication_e

####################################################################################################################
CREATE TABLE  `your-project-id1234.N01_NDC.y21`  as 
select distinct
a.medication_e,
a.Ndc,
b.Proprietary_Name_rep as rank

from
(SELECT distinct medication_e, Ndc FROM `your-project-id1234.N01_NDC.t4` 
where Ndc is not null and Ndc <> "0" and Ndc <> "" 
and medication_e<>"___" and  medication_e is not null
and medication_e<>"0") a

left join  `your-project-id1234.N01_NDC.x3` b
on a.Ndc=b.Ndc;

####################################################################################################################

CREATE TABLE  `your-project-id1234.N01_NDC.y22`  as 
select medication_e, Ndc from
(select distinct
a.medication_e,
a.Ndc,
b.rnk

from `your-project-id1234.N01_NDC.y21` a


left join
(select medication_e, max(rank) as rnk from
(SELECT medication_e, rank FROM `your-project-id1234.N01_NDC.y21` )
group by medication_e) b
on a.medication_e=b.medication_e and a.rank=b.rnk )
where rnk is not null;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y23`  AS

SELECT
a.medication_e, a.Ndc,
b.RankN,

FROM `your-project-id1234.N01_NDC.y22`  a
LEFT   JOIN (
SELECT  
medication_e, Ndc,
Row_number() over (partition by  medication_e order by Ndc asc) as RankN
FROM

(SELECT   DISTINCT medication_e, Ndc  FROM `your-project-id1234.N01_NDC.y22`  )  ) b
ON
a.medication_e = b.medication_e AND a.Ndc = b.Ndc;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y24`  AS
SELECT medication_e, Ndc FROM `your-project-id1234.N01_NDC.y23` 
where RankN=1;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
