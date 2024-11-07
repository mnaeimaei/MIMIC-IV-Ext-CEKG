import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

#### Create GSN

####################################################################################################################
CREATE TABLE  `your-project-id1234.N01_NDC.y61`  as 
select distinct
a.Gsn,
a.Ndc,
b.Proprietary_Name_rep as rank

from
(SELECT distinct Gsn, Ndc FROM `your-project-id1234.N01_NDC.t4` 
where Ndc is not null and Ndc <> "0" and Ndc <> "" and Gsn<>"___" and  Gsn is not null and Gsn<>"000" ) a

left join  `your-project-id1234.N01_NDC.x3` b
on a.Ndc=b.Ndc;

####################################################################################################################

CREATE TABLE  `your-project-id1234.N01_NDC.y62`  as 
select Gsn, Ndc from
(select distinct
a.Gsn,
a.Ndc,
b.rnk

from `your-project-id1234.N01_NDC.y61` a


left join
(select Gsn, max(rank) as rnk from
(SELECT Gsn, rank FROM `your-project-id1234.N01_NDC.y61` )
group by Gsn) b
on a.Gsn=b.Gsn and a.rank=b.rnk )
where rnk is not null;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y63`  AS

SELECT
a.Gsn, a.Ndc,
b.RankN,

FROM `your-project-id1234.N01_NDC.y62`  a
LEFT   JOIN (
SELECT  
Gsn, Ndc,
Row_number() over (partition by  Gsn order by Ndc asc) as RankN
FROM

(SELECT   DISTINCT Gsn, Ndc  FROM `your-project-id1234.N01_NDC.y62`  )  ) b
ON
a.Gsn = b.Gsn AND a.Ndc = b.Ndc;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y64`  AS
SELECT Gsn, Ndc FROM `your-project-id1234.N01_NDC.y63` 
where RankN=1;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
