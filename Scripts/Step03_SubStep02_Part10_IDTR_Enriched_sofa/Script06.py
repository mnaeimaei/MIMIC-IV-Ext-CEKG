import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



CREATE TABLE  `your-project-id1234.N16_sofa.X_antibioticA1`   AS
select * From `your-project-id1234.N16_sofa.X_antibiotic` 
where stay_id is null;

CREATE TABLE  `your-project-id1234.N16_sofa.X_antibioticB1`   AS
select * From `your-project-id1234.N16_sofa.X_antibiotic` 
where stay_id is not null;

#####################################################################

CREATE TABLE  `your-project-id1234.N16_sofa.X_suspected_infectionA1`   AS
select * From `your-project-id1234.N16_sofa.X_suspected_infection` 
where stay_id is null;

CREATE TABLE  `your-project-id1234.N16_sofa.X_suspected_infectionB1`   AS
select * From `your-project-id1234.N16_sofa.X_suspected_infection` 
where stay_id is not null;

#####################################################################

CREATE TABLE  `your-project-id1234.N16_sofa.X_cultureA1`   AS
select * From `your-project-id1234.N16_sofa.X_culture` 
where stay_id is null;

CREATE TABLE  `your-project-id1234.N16_sofa.X_cultureB1`   AS
select * From `your-project-id1234.N16_sofa.X_culture` 
where stay_id is not null;
        
'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
