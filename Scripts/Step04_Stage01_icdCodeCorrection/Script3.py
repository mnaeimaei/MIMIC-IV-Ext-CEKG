

import os
import pandas as pd
from google.cloud import bigquery

import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query:

query1 = f'''    

 `mimic-four-ml4219.O_NonEvents_ICD7.icdCM1_short` 

'''
QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

df = query_job.to_dataframe()
symPath2 = '1_ICD.csv'
Realpath2 = os.path.realpath(symPath2)
df2 = df.to_csv(Realpath2, index=False)

