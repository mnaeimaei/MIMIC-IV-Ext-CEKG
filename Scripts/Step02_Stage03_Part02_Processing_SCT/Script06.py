import os
from google.cloud import bigquery

symPath = '../gcKey/SNOMED_CT_Google_Cloud.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE  `snomed-ct-ml4219.K05_SCT.B_2`   AS  
SELECT * FROM `snomed-ct-ml4219.K05_SCT.A_Description` ;


alter table `snomed-ct-ml4219.K05_SCT.B_2` 
add column ConceptType string;

update `snomed-ct-ml4219.K05_SCT.B_2` 
set ConceptType="Concept"
where ConceptType is null;

update `snomed-ct-ml4219.K05_SCT.B_2` 
set ConceptType="Root"
where conceptId=138875005;

update `snomed-ct-ml4219.K05_SCT.B_2` 
set ConceptType="TLC"
where conceptId=123037004
or conceptId=404684003
or conceptId=308916002
or conceptId=272379006
or conceptId=363787002
or conceptId=410607006
or conceptId=373873005
or conceptId=78621006
or conceptId=260787004
or conceptId=71388002
or conceptId=362981000
or conceptId=419891008
or conceptId=243796009
or conceptId=900000000000441003
or conceptId=48176007
or conceptId=370115009
or conceptId=123038009
or conceptId=254291000
or conceptId=105590001;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
