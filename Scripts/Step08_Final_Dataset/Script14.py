import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        
CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.M_CNM5_Activity_Instance_ID`  AS
SELECT 
Activity_Value_ID as Activity_Instance_ID
FROM `your-project-id1234.I17_DK7.TabA` ;

#######################################################

CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.M_CNM5_Activity_Instance_ID_with_class_Entities`  AS
SELECT
Activity_Value_ID as Activity_Instance_ID,
Disorders as Disorders_Name,
subject_id	temp_patient_id ,
hadm_id temp_encounter_id
FROM `your-project-id1234.I17_DK7.TabA_class_mainEntities` ;


#######################################################

CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.M_CNM5_Activity_Instance_ID_with_features_Entities`  AS
SELECT 

Activity_Value_ID as Activity_Instance_ID,
Activity,
Activity_Synonym,
featureName as Activity_Attribute,
featureValue as Activity_Attribute_Value,
subject_id	temp_patient_id ,
hadm_id temp_encounter_id,
Timestamp
FROM `your-project-id1234.I17_DK7.TabA_features_mainEntities` ;


###########################################################



CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.M_CNM5_class`  AS
SELECT *
FROM `your-project-id1234.I17_DK7.TabA_Disorders` ;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
