import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE `your-project-id1234.N01_NDC.w5`  AS
        SELECT distinct
        NdcID, Ndc, Gsn, Drug,prod_strength 
        from  `your-project-id1234.N01_NDC.w4` 
        where RankN=1


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
