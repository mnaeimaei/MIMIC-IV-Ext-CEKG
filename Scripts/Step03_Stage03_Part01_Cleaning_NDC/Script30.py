import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

#### Create Product Code

####################################################################################################################
CREATE TABLE  `your-project-id1234.N01_NDC.y01`  as 
select distinct
a.product_code,
a.Ndc,
b.Proprietary_Name_rep as rank

from
(SELECT distinct product_code, Ndc FROM `your-project-id1234.N01_NDC.t4` 
where Ndc is not null and Ndc <> "0" and Ndc <> "" and product_code<>"___" and  product_code is not null) a

left join  `your-project-id1234.N01_NDC.x3` b
on a.Ndc=b.Ndc;

####################################################################################################################

CREATE TABLE  `your-project-id1234.N01_NDC.y02`  as 
select product_code, Ndc from
(select distinct
a.product_code,
a.Ndc,
b.rnk

from `your-project-id1234.N01_NDC.y01` a


left join
(select product_code, max(rank) as rnk from
(SELECT product_code, rank FROM `your-project-id1234.N01_NDC.y01` )
group by product_code) b
on a.product_code=b.product_code and a.rank=b.rnk )
where rnk is not null;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y03`  AS

SELECT
a.product_code, a.Ndc,
b.RankN,

FROM `your-project-id1234.N01_NDC.y02`  a
LEFT   JOIN (
SELECT  
product_code, Ndc,
Row_number() over (partition by  product_code order by Ndc asc) as RankN
FROM

(SELECT   DISTINCT product_code, Ndc  FROM `your-project-id1234.N01_NDC.y02`  )  ) b
ON
a.product_code = b.product_code AND a.Ndc = b.Ndc;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y04`  AS
SELECT product_code, Ndc FROM `your-project-id1234.N01_NDC.y03` 
where RankN=1;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
