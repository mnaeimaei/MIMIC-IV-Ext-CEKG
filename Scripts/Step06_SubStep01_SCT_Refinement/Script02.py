import os
from google.cloud import bigquery

symPath = '../gcKey/SNOMED_CT_Google_Cloud.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

###################################
################################### Go to SQL \ Query 10_SCT
###################################
# Perform a query.
query1 = f'''            


CREATE TABLE  `your-project-id1234.u1.SNOMED`   AS  
select distinct * from(
SELECT distinct CAST(SCDID AS INT64) OTC FROM `your-project-id1234.K03_SCT.B_ICD` 
union all
SELECT distinct OTC FROM `your-project-id1234.K03_SCT.B_Domain` 
union all
SELECT distinct OTC  FROM `your-project-id1234.K03_SCT.C_Activity` );


CREATE TABLE  `your-project-id1234.u1.TTTT_1`   AS
SELECT DISTINCT
sourceId as s0, destinationId as s1
From `your-project-id1234.K05_SCT.REL`   
WHERE sourceId in (select * from   `your-project-id1234.u1.SNOMED`  )
;



#Test, if "There is no data to display." it is ok
select distinct OTC from
(SELECT distinct
a.OTC,
b.s0
FROM `your-project-id1234.u1.SNOMED` a
left join (SELECT distinct s0 FROM `your-project-id1234.u1.TTTT_1` ) b
on a.OTC=b.s0)
where s0 is null;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
