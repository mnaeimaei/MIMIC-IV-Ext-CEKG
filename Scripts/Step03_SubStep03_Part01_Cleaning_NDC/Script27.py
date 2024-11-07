

########Dimention Reduction

import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import os

symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)
credentials = service_account.Credentials.from_service_account_file(Realpath)

projectName="your-project-id1234"
databaeName="N01_NDC"
TableName="x1"

DesTab=databaeName + '.' + TableName
proj_id=projectName


# dataframe Cluster K=2
symPath = 'Script27_Data_Final.csv'
Realpath = os.path.realpath(symPath)
df = pd.read_csv(Realpath)

# write the dataframe to BigQuery
pandas_gbq.to_gbq(df, destination_table=DesTab, project_id=proj_id, credentials=credentials, if_exists='replace')
