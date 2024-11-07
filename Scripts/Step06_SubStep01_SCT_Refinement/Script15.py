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

CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperG`   AS 
SELECT distinct
a.s0 as conceptId,
a.s0 as conceptCode,
b.termA,
b.termA_p1,
b.termA_p2,
b.termB,
b.ConceptType,
a.P0 as level
FROM `snomed-ct-ml4219.K06_SCT.PaperF` a
left join `snomed-ct-ml4219.K05_SCT.NODES` b
on
a.s0=b.conceptId;

update  `snomed-ct-ml4219.K06_SCT.PaperG`   
set conceptCode=1
where conceptCode is not null;







'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
