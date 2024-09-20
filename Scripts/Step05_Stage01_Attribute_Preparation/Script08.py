import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import os

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)
credentials = service_account.Credentials.from_service_account_file(Realpath)

projectName = "your-project-id1234"
databaeName = "O_NonEvents_ICD3"
TableName = "icdCM10"

DesTab = databaeName + '.' + TableName
proj_id = projectName

symPath = symPath2 = 'Script08_icd10_Syn.csv'
Realpath = os.path.realpath(symPath)
df = pd.read_csv(Realpath)

pandas_gbq.to_gbq(df, destination_table=DesTab, project_id=proj_id, credentials=credentials, if_exists='replace')
