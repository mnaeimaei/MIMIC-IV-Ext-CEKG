import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.H_SCT_Node`  AS
SELECT 
conceptId as SCT_ID,
conceptCode as SCT_Code,
termA as SCT_DescriptionA_Type1,
termA_p1 as SCT_DescriptionA_Type2,
termB as  SCT_DescriptionB,
termA_p2 as SCT_Semantic_Tags,
ConceptType as  SCT_Type,
level as SCT_Level,
FROM `your-project-id1234.I071_octNode.TabA` ;



CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.H_SCT_REL`  AS
SELECT 
s0 as  SCT_ID_1,
p0 as SCT_Code_1,
s1 as SCT_ID_2,
p1 as  SCT_Code_2
FROM `your-project-id1234.I072_octRel.TabA` ;
'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
