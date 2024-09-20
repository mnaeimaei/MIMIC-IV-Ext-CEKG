import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



ALTER TABLE  `your-project-id1234.I01_Log.TabE02` 
ADD COLUMN Ent1 STRING;

update `your-project-id1234.I01_Log.TabE02` 
set Ent1="Patient"
where Ent1 is null;

###########################################################
ALTER TABLE  `your-project-id1234.I01_Log.TabE02` 
ADD COLUMN Ent2 STRING;

update `your-project-id1234.I01_Log.TabE02` 
set Ent2="Admission"
where Ent2 is null;

###########################################################



ALTER TABLE  `your-project-id1234.I01_Log.TabE02` 
ADD COLUMN Ent3 STRING;

update `your-project-id1234.I01_Log.TabE02` 
set Ent3="Outpatient"
where Ent3 is null;

###########################################################

ALTER TABLE  `your-project-id1234.I01_Log.TabE02` 
ADD COLUMN Ent4 STRING;



update `your-project-id1234.I01_Log.TabE02` 
set Ent4="Admission_Sequence"
where Ent4 is null;  



###########################################################
ALTER TABLE  `your-project-id1234.I01_Log.TabE02` 
ADD COLUMN Ent5 STRING;


update `your-project-id1234.I01_Log.TabE02` 
set Ent5="Outpatient_Sequence"
where Ent5 is null;  








'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
