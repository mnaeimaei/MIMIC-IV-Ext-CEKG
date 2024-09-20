import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        
CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.E_ActivityAttributes`  AS
SELECT 
Activity_Properties_ID as Activity_Attributes_ID,
Activity	,
Activity_Synonym	,
featureName as Attribute,
featureValue as Attribute_Value                
FROM `your-project-id1234.I04_actPro.TabA1` ;

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
