import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create table `your-project-id1234.N03_OMR.01_Weight` as
SELECT * FROM `your-project-id1234.N03_OMR.OMR13` 
where result_name="Weight (Lbs)" or result_name="Weight";

create table `your-project-id1234.N03_OMR.02_Height` as
SELECT * FROM `your-project-id1234.N03_OMR.OMR13` 
where result_name="Height (Inches)" or result_name="Height";

create table `your-project-id1234.N03_OMR.03_eGFR` as
SELECT * FROM `your-project-id1234.N03_OMR.OMR13` 
where result_name="eGFR";

create table `your-project-id1234.N03_OMR.04_BMI` as
SELECT * FROM `your-project-id1234.N03_OMR.OMR13` 
where result_name="BMI (kg/m2)" or result_name="BMI";

create table `your-project-id1234.N03_OMR.05_BP` as
SELECT * FROM `your-project-id1234.N03_OMR.OMR13` 
where result_name="Blood Pressure"
or result_name="Blood Pressure Standing"
or result_name="Blood Pressure Sitting"
or result_name="Blood Pressure Standing (1 min)"
or result_name="Blood Pressure Lying"
or result_name="Blood Pressure Standing (3 mins)";



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
