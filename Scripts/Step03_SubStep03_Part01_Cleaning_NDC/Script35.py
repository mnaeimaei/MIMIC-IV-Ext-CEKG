import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

#### Create formulary_drug_cd

####################################################################################################################
CREATE TABLE  `your-project-id1234.N01_NDC.y51`  as 
select distinct
a.formulary_drug_cd,
a.Ndc,
b.Proprietary_Name_rep as rank

from
(SELECT distinct formulary_drug_cd, Ndc FROM `your-project-id1234.N01_NDC.t4` 
where Ndc is not null and Ndc <> "0" and Ndc <> "" and formulary_drug_cd<>"___" and  formulary_drug_cd is not null
and formulary_drug_cd<>"0" and  formulary_drug_cd<>"ever10" and  formulary_drug_cd<>"000" 
order by formulary_drug_cd) a

left join  `your-project-id1234.N01_NDC.x3` b
on a.Ndc=b.Ndc;

####################################################################################################################

CREATE TABLE  `your-project-id1234.N01_NDC.y52`  as 
select formulary_drug_cd, Ndc from
(select distinct
a.formulary_drug_cd,
a.Ndc,
b.rnk

from `your-project-id1234.N01_NDC.y51` a


left join
(select formulary_drug_cd, max(rank) as rnk from
(SELECT formulary_drug_cd, rank FROM `your-project-id1234.N01_NDC.y51` )
group by formulary_drug_cd) b
on a.formulary_drug_cd=b.formulary_drug_cd and a.rank=b.rnk )
where rnk is not null;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y53`  AS

SELECT
a.formulary_drug_cd, a.Ndc,
b.RankN,

FROM `your-project-id1234.N01_NDC.y52`  a
LEFT   JOIN (
SELECT  
formulary_drug_cd, Ndc,
Row_number() over (partition by  formulary_drug_cd order by Ndc asc) as RankN
FROM

(SELECT   DISTINCT formulary_drug_cd, Ndc  FROM `your-project-id1234.N01_NDC.y52`  )  ) b
ON
a.formulary_drug_cd = b.formulary_drug_cd AND a.Ndc = b.Ndc;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y54`  AS
SELECT formulary_drug_cd, Ndc FROM `your-project-id1234.N01_NDC.y53` 
where RankN=1;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
