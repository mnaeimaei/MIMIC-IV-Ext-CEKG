import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

#### Create medication_p

####################################################################################################################
CREATE TABLE  `your-project-id1234.N01_NDC.y11`  as 
select distinct
a.medication_p,
a.Ndc,
b.Proprietary_Name_rep as rank

from
(SELECT distinct medication_p, Ndc FROM `your-project-id1234.N01_NDC.t4` 
where Ndc is not null and Ndc <> "0" and Ndc <> "" 
and medication_p<>"___" and  medication_p is not null
and medication_p<>"000" and  medication_p<>"*NF*" and  medication_p<>"*NF"
and medication_p<>"*nf" and  medication_p<>"*nf* " and  medication_p<>"1"
and medication_p<>"2" and  medication_p<>"3" ) a

left join  `your-project-id1234.N01_NDC.x3` b
on a.Ndc=b.Ndc;

####################################################################################################################

CREATE TABLE  `your-project-id1234.N01_NDC.y12`  as 
select medication_p, Ndc from
(select distinct
a.medication_p,
a.Ndc,
b.rnk

from `your-project-id1234.N01_NDC.y11` a


left join
(select medication_p, max(rank) as rnk from
(SELECT medication_p, rank FROM `your-project-id1234.N01_NDC.y11` )
group by medication_p) b
on a.medication_p=b.medication_p and a.rank=b.rnk )
where rnk is not null;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y13`  AS

SELECT
a.medication_p, a.Ndc,
b.RankN,

FROM `your-project-id1234.N01_NDC.y12`  a
LEFT   JOIN (
SELECT  
medication_p, Ndc,
Row_number() over (partition by  medication_p order by Ndc asc) as RankN
FROM

(SELECT   DISTINCT medication_p, Ndc  FROM `your-project-id1234.N01_NDC.y12`  )  ) b
ON
a.medication_p = b.medication_p AND a.Ndc = b.Ndc;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y14`  AS
SELECT medication_p, Ndc FROM `your-project-id1234.N01_NDC.y13` 
where RankN=1;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
