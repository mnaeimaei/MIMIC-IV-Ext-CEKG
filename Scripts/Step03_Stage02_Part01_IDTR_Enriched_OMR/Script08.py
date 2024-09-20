import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.N03_OMR.OMR071`  AS
SELECT * from your-project-id1234.N03_OMR.OMR061
where RankN=1;


############################################################


CREATE TABLE `your-project-id1234.N03_OMR.OMR072`  AS
SELECT * FROM `your-project-id1234.N03_OMR.OMR062` ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
