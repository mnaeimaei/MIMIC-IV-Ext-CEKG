

import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import os

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)
credentials = service_account.Credentials.from_service_account_file(Realpath)

projectName = "your-project-id1234"
databaeName = "K04_SCT"
TableName = "Terminology_Description24"

DesTab = databaeName + '.' + TableName
proj_id = projectName


symPath = symPath2 = 'Script02_Description.txt'
Realpath = os.path.realpath(symPath)

#SnomedCT_InternationalRF2_PRODUCTION_20240601T120000Z /   Full  / Terminology  / sct2_Description_Full-en_INT_20240601.txt

df = pd.read_csv(Realpath, delimiter='\t')  # assuming tab-separated values

pandas_gbq.to_gbq(df, destination_table=DesTab, project_id=proj_id, credentials=credentials, if_exists='replace')
