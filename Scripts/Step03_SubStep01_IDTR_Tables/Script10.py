import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



###########################################################################

update `your-project-id1234.R_TimeA.Y_S` 
set min=mini_Time
where mini_Date is null;

update `your-project-id1234.R_TimeA.Y_S` 
set max=maxi_Time
where maxi_Date is null;


update `your-project-id1234.R_TimeA.Y_S` 
set min=mini_Time
where mini_Date is not null and mini_Date<mini_Time;

update `your-project-id1234.R_TimeA.Y_S` 
set max=maxi_Time
where maxi_Date is not null and maxi_Date>maxi_Time;


update `your-project-id1234.R_TimeA.Y_S` 
set min=mini_Time
where mini_Date is not null and mini_Date>=mini_Time;


update `your-project-id1234.R_TimeA.Y_S` 
set max=maxi_Time
where maxi_Date is not null and maxi_Date<=maxi_Time;

#######################################################

update `your-project-id1234.R_TimeA.Y_SH` 
set min=mini_Time
where mini_Date is null;

update `your-project-id1234.R_TimeA.Y_SH` 
set max=maxi_Time
where maxi_Date is null;


update `your-project-id1234.R_TimeA.Y_SH` 
set min=mini_Time
where mini_Date is not null and mini_Date<mini_Time;

update `your-project-id1234.R_TimeA.Y_SH` 
set max=maxi_Time
where maxi_Date is not null and maxi_Date>maxi_Time;


update `your-project-id1234.R_TimeA.Y_SH` 
set min=mini_Time
where mini_Date is not null and mini_Date>=mini_Time;


update `your-project-id1234.R_TimeA.Y_SH` 
set max=maxi_Time
where maxi_Date is not null and maxi_Date<=maxi_Time;

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
