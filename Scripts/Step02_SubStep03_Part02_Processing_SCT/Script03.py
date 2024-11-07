import os
from google.cloud import bigquery

symPath = '../gcKey/SNOMED_CT_Google_Cloud.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `snomed-ct-ml4219.K05_SCT.A_Description2_new`   AS
        select distinct * from

        (SELECT a.conceptId, a.term from

        (SELECT distinct conceptId,effectiveTime, term  
        FROM `snomed-ct-ml4219.K04_SCT.Terminology_Description24` 
        where typeId=900000000000003001  and active=1     and term like "%)"
        order by conceptId ) a

        inner join (
        SELECT distinct conceptId, MAX(effectiveTime) AS MaxEffectiveTime  
        FROM `snomed-ct-ml4219.K04_SCT.Terminology_Description24` 
        where typeId=900000000000003001  and active=1     and term like "%)"
        GROUP BY conceptId)b

        ON a.conceptId = b.conceptId AND a.effectiveTime = b.MaxEffectiveTime);

    #######################################################################################

    CREATE TABLE  `snomed-ct-ml4219.K02_SCT.A_Description`   AS
SELECT conceptId, term,
REGEXP_EXTRACT(term, r'^(.*?)\([^()]+\)$') AS termA_p1,
REGEXP_REPLACE(term, r'^.*\(([^()]+)\)$', r'\1') AS termA_p2
FROM `snomed-ct-ml4219.K05_SCT.A_Description2_new` ;

#########################################################################

        drop TABLE  `snomed-ct-ml4219.K05_SCT.A_Description2_new`   ;

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
