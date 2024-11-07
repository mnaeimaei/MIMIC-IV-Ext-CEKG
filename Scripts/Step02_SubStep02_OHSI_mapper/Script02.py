
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import os

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)
credentials = service_account.Credentials.from_service_account_file(Realpath)

projectName = "your-project-id1234"
databaeName = "K02_SCT"
TableName = "Concept"

DesTab = databaeName + '.' + TableName
proj_id = projectName

symPath = symPath2 = 'Script02_CONCEPT.csv'
Realpath = os.path.realpath(symPath)


# Define your schema with column names

column_names = ["concept_id", "concept_name", "domain_id", "vocabulary_id", "concept_class_id", "standard_concept", "concept_code", "valid_start_date", "valid_end_date", "invalid_reason"]


# Define data types for each column (order should match column_names)
column_data_types = {'concept_id': int ,'concept_name': str ,'domain_id': str ,'vocabulary_id': str ,'concept_class_id': str ,'standard_concept': str ,'concept_code': str ,'valid_start_date': str  ,'valid_end_date': str ,'invalid_reason': str  }

# Use the read_csv method with dtype parameter to specify column data types
df = pd.read_csv(Realpath, delimiter=',', names=column_names, dtype=column_data_types)



pandas_gbq.to_gbq(df, destination_table=DesTab, project_id=proj_id, credentials=credentials, if_exists='replace')



