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


Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p0 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p1 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p2 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p3 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p4 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p5 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p6 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p7 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p8 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p9 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p10 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p11 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p12 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p13 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p14 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p15 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p16 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p17 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p18 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p19 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p20 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p21 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p22 int;


Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p23 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p24 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p25 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p26 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p27 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p28 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p29 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p30 int;

Alter table `snomed-ct-ml4219.K06_SCT.PaperB` 
add column p31 int;







'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
