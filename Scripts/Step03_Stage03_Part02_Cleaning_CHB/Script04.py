
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import os

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)
credentials = service_account.Credentials.from_service_account_file(Realpath)

projectName = "your-project-id1234"
databaeName = "N04_CHB"
TableName = "a_Conv"

DesTab = databaeName + '.' + TableName
proj_id = projectName

symPath = symPath2 = 'Script04_Data.csv'
Realpath = os.path.realpath(symPath)


# Define your schema with column names

column_names = ['Category', 'original_fluid', 'new_fluid', 'original_lable', 'new_Label', 'Short', 'Unit', 'Normal_Range']
# Define data types for each column (order should match column_names)
column_data_types = {'Category': str ,'original_fluid': str ,'new_fluid': str ,'original_lable': str ,'new_Label': str ,'Short': str ,'Unit': str ,'Normal_Range': str }

# Use the read_csv method with dtype parameter to specify column data types
df = pd.read_csv(Realpath, delimiter=',', names=column_names, dtype=column_data_types)



pandas_gbq.to_gbq(df, destination_table=DesTab, project_id=proj_id, credentials=credentials, if_exists='replace')



