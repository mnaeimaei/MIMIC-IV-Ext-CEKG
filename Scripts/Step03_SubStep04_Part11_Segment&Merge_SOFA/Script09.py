import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.N16_sofa.X_antibioticA4`  AS
SELECT * FROM `your-project-id1234.N16_sofa.X_antibioticA3` 
where RankN is null or RankN=1;

CREATE TABLE `your-project-id1234.N16_sofa.X_cultureA4`  AS
SELECT * FROM `your-project-id1234.N16_sofa.X_cultureA3` 
where RankN is null or RankN=1;

CREATE TABLE `your-project-id1234.N16_sofa.X_suspected_infectionA4`  AS
SELECT * FROM `your-project-id1234.N16_sofa.X_suspected_infectionA3` 
where RankN is null or RankN=1;
'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
