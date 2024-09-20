import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

update  `your-project-id1234.N06_VASO.TabA3`   
set dobutamine_vaso_rate=dobutamine
where dobutamine_vaso_rate is null;

update  `your-project-id1234.N06_VASO.TabA3`   
set dopamine_vaso_rate=dopamine
where dopamine_vaso_rate is null;

update  `your-project-id1234.N06_VASO.TabA3`   
set epinephrine_vaso_rate=epinephrine
where epinephrine_vaso_rate is null;

update  `your-project-id1234.N06_VASO.TabA3`   
set milrinone_vaso_rate=milrinone
where milrinone_vaso_rate is null;

update  `your-project-id1234.N06_VASO.TabA3`   
set norepinephrine_vaso_rate=norepinephrine
where norepinephrine_vaso_rate is null;

update  `your-project-id1234.N06_VASO.TabA3`   
set phenylephrine_vaso_rate=phenylephrine
where phenylephrine_vaso_rate is null;

update  `your-project-id1234.N06_VASO.TabA3`   
set vasopressin_vaso_rate=vasopressin
where vasopressin_vaso_rate is null;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
