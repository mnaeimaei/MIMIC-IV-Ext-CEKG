

import os
import pandas as pd
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query:

query1 = f'''    

SELECT distinct Name, tempValue FROM `your-project-id1234.O_NonEvents_ICD3.icdCM09` 

'''
QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

df = query_job.to_dataframe()
symPath2 = 'Script07_Output.csv'
Realpath2 = os.path.realpath(symPath2)
df2 = df.to_csv(Realpath2, index=False)
