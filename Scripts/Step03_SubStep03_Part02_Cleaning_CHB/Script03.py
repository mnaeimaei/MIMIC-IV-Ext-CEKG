
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

SELECT category, fluid as original_fluid, label as original_lable FROM `your-project-id1234.N04_CHB.a_Lab` 

'''
QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

df = query_job.to_dataframe()
symPath2 = 'Script03_Data.csv'
Realpath2 = os.path.realpath(symPath2)
df2 = df.to_csv(Realpath2, index=False)

