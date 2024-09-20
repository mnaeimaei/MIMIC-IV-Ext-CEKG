import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

#### Create product_description

####################################################################################################################
CREATE TABLE  `your-project-id1234.N01_NDC.y41`  as 
select distinct
a.product_description,
a.Ndc,
b.Proprietary_Name_rep as rank

from
(SELECT distinct product_description, Ndc FROM `your-project-id1234.N01_NDC.t4` 
where Ndc is not null and Ndc <> "0" and Ndc <> "" and product_description<>"___" and  product_description is not null
and product_description<>"0" and  product_description<>"000" and  product_description<>"*NF" and  product_description<>"*nf"
and  product_description<>"*nf* " and  product_description<>"*nf*" and  product_description<>"1" and  product_description<>"2"
) a

left join  `your-project-id1234.N01_NDC.x3` b
on a.Ndc=b.Ndc;

####################################################################################################################

CREATE TABLE  `your-project-id1234.N01_NDC.y42`  as 
select product_description, Ndc from
(select distinct
a.product_description,
a.Ndc,
b.rnk

from `your-project-id1234.N01_NDC.y41` a


left join
(select product_description, max(rank) as rnk from
(SELECT product_description, rank FROM `your-project-id1234.N01_NDC.y41` )
group by product_description) b
on a.product_description=b.product_description and a.rank=b.rnk )
where rnk is not null;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y43`  AS

SELECT
a.product_description, a.Ndc,
b.RankN,

FROM `your-project-id1234.N01_NDC.y42`  a
LEFT   JOIN (
SELECT  
product_description, Ndc,
Row_number() over (partition by  product_description order by Ndc asc) as RankN
FROM

(SELECT   DISTINCT product_description, Ndc  FROM `your-project-id1234.N01_NDC.y42`  )  ) b
ON
a.product_description = b.product_description AND a.Ndc = b.Ndc;

####################################################################################################################

CREATE TABLE `your-project-id1234.N01_NDC.y44`  AS
SELECT product_description, Ndc FROM `your-project-id1234.N01_NDC.y43` 
where RankN=1;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
