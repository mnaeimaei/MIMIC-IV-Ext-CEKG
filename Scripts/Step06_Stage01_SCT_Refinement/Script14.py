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


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperH`   AS 
SELECT *  FROM `snomed-ct-ml4219.K06_SCT.PaperC` ;

update `snomed-ct-ml4219.K06_SCT.PaperH` 
set p0=1
where p0 is not null;


update `snomed-ct-ml4219.K06_SCT.PaperH` 
set p1=1
where p1 is not null;

CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperI`   AS 
SELECT distinct *  FROM `snomed-ct-ml4219.K06_SCT.PaperH` ;

drop table `snomed-ct-ml4219.K06_SCT.PaperH` ;

CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperH`   AS 
SELECT distinct *  FROM `snomed-ct-ml4219.K06_SCT.PaperI` ;

drop table `snomed-ct-ml4219.K06_SCT.PaperI` ;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
