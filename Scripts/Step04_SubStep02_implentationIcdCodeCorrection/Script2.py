import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create table    `your-project-id1234.O_NonEvents_ICD6.icdCM2` as
SELECT 
a.subject_id,
a.hadm_id,
a.seq_num,
a.icd_code as icd_code_first,
a.icd_version as icd_version_first,
a.long_title,
b.ICD9_Code as ICD9,
b.ICD10,
b.ICD10_Root,
b.ICD10_Root_title

FROM `your-project-id1234.O_NonEvents_ICD6.icdCM1` a
left join  `your-project-id1234.P_NonEvents_ICD7.Mapper` b
on a.icd_code=b.icd_code
and a.long_title=b.long_title
and a.icd_version=icd_verstion;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
