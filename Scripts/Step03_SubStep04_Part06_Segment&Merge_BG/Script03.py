import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

alter table `your-project-id1234.N08_BG.first_day_bg_time`  
add column Type string;


alter table `your-project-id1234.N08_BG.first_day_bg_art_time`  
add column Type string;

update `your-project-id1234.N08_BG.first_day_bg_time`  
set Type = "NA"
where Type is null;


update `your-project-id1234.N08_BG.first_day_bg_art_time`  
set Type = "ART"
where Type is null;



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
