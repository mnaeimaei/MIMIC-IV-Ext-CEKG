import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE  `your-project-id1234.N16_sofa.X_antibiotic`   AS
SELECT distinct
subject_id, stay_id, hadm_id,antibiotic_time  as Timestamps,  ab_id, antibiotic
FROM `your-project-id1234.N16_sofa.suspicion_of_infection` 
where antibiotic_time is not null;

CREATE TABLE  `your-project-id1234.N16_sofa.X_suspected_infection`   AS
SELECT distinct
 subject_id, stay_id, hadm_id, suspected_infection_time  as Timestamps, suspected_infection
FROM `your-project-id1234.N16_sofa.suspicion_of_infection` 
where suspected_infection_time is not null;


CREATE TABLE  `your-project-id1234.N16_sofa.X_culture`   AS
SELECT distinct
 subject_id, stay_id, hadm_id,culture_time  as Timestamps, specimen, positive_culture
FROM `your-project-id1234.N16_sofa.suspicion_of_infection` 
where culture_time is not null;

##################################################################################################################




'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
