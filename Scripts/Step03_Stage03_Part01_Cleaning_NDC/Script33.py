import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

#### Create 	Drug

####################################################################################################################
CREATE TABLE  `your-project-id1234.N01_NDC.y31`  as 
select distinct
a.Drug,
a.Ndc,
b.Proprietary_Name_rep as rank

from
(SELECT distinct Drug, Ndc FROM `your-project-id1234.N01_NDC.t4` 
where Ndc is not null and Ndc <> "0" and Ndc <> "" and Drug<>"___" and  Drug is not null
and Drug<>"0" and  Drug<>"000" and  Drug<>"*NF" and  Drug<>"*nf" and  Drug<>"*nf* " and  Drug<>"*nf*" and  Drug<>"1" and  Drug<>"2") a

left join  `your-project-id1234.N01_NDC.x3` b
on a.Ndc=b.Ndc;

####################################################################################################################

CREATE TABLE  `your-project-id1234.N01_NDC.y32`  as 
select Drug, Ndc from
(select distinct
a.Drug,
a.Ndc,
b.rnk

from `your-project-id1234.N01_NDC.y31` a


left join
(select Drug, max(rank) as rnk from
(SELECT Drug, rank FROM `your-project-id1234.N01_NDC.y31` )
group by Drug) b
on a.Drug=b.Drug and a.rank=b.rnk )
where rnk is not null;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y33`  AS

SELECT
a.Drug, a.Ndc,
b.RankN,

FROM `your-project-id1234.N01_NDC.y32`  a
LEFT   JOIN (
SELECT  
Drug, Ndc,
Row_number() over (partition by  Drug order by Ndc asc) as RankN
FROM

(SELECT   DISTINCT Drug, Ndc  FROM `your-project-id1234.N01_NDC.y32`  )  ) b
ON
a.Drug = b.Drug AND a.Ndc = b.Ndc;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y34`  AS
SELECT Drug, Ndc FROM `your-project-id1234.N01_NDC.y33` 
where RankN=1;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
