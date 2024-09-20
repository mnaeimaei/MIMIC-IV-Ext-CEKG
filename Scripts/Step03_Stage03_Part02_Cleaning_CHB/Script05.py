import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



        CREATE TABLE  `your-project-id1234.N04_CHB.b_Lab`   AS
        SELECT * FROM (
        SELECT
        a.subject_id , a.hadm_id , a.labevent_id , a.specimen_id , a.itemid , a.order_provider_id , a.charttime , a.storetime , a.value , a.valuenum , a.valueuom , a.ref_range_lower , a.ref_range_upper , a.flag , a.priority , a.comments , a.label , a.fluid , a.category,
        b.new_fluid , b.new_Label , b.Short as short_Label , b.Unit as new_valueuom , b.Normal_Range
        From `your-project-id1234.N04_CHB.a_Lab`   as a
        LEFT JOIN `your-project-id1234.N04_CHB.a_Conv`   as b
        ON
        a.category=b.Category AND a.fluid=b.original_fluid AND a.label=b.original_lable )        ; 



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
