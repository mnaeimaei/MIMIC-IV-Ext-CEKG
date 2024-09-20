import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create table  `your-project-id1234.N04_CHB.xLab01`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Arterial blood sample analysis - hemoglobin" ;
create table  `your-project-id1234.N04_CHB.xLab02`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Arterial blood sample analysis - plasma" ;
create table  `your-project-id1234.N04_CHB.xLab03`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Arterial blood sample analysis - whole" ;
create table  `your-project-id1234.N04_CHB.xLab04`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Ascite Sample analysis - Red Blood Cells" ;
create table  `your-project-id1234.N04_CHB.xLab05`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Ascite Sample analysis - White Blood Cells" ;
create table  `your-project-id1234.N04_CHB.xLab06`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Ascite Sample analysis - Whole" ;
create table  `your-project-id1234.N04_CHB.xLab07`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Bone Marrow Sample analysis - Nucleated Cells" ;
create table  `your-project-id1234.N04_CHB.xLab08`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Bone Marrow Sample analysis - Plasma" ;
create table  `your-project-id1234.N04_CHB.xLab09`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Bone Marrow Sample analysis - White Blood Cells" ;
create table  `your-project-id1234.N04_CHB.xLab10`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Bone Marrow Sample analysis - Whole" ;
create table  `your-project-id1234.N04_CHB.xLab11`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Cerebrospinal Sample analysis - White Blood Cells" ;
create table  `your-project-id1234.N04_CHB.xLab12`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Cerebrospinal Sample analysis - Whole" ;
create table  `your-project-id1234.N04_CHB.xLab13`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Joint Fluid sample analysis - White Blood Cells" ;
create table  `your-project-id1234.N04_CHB.xLab14`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Joint Fluid sample analysis - Whole" ;
create table  `your-project-id1234.N04_CHB.xLab15`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Lab Test" ;
create table  `your-project-id1234.N04_CHB.xLab16`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Other Body Fluid Analysis" ;
create table  `your-project-id1234.N04_CHB.xLab17`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Peripheral Blood Sample analysis - Creatine Kinase" ;
create table  `your-project-id1234.N04_CHB.xLab18`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Peripheral Blood Sample analysis - Hemoglobin" ;
create table  `your-project-id1234.N04_CHB.xLab19`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Peripheral Blood Sample analysis - lymphocyte" ;
create table  `your-project-id1234.N04_CHB.xLab20`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Peripheral Blood Sample analysis - Plasma" ;
create table  `your-project-id1234.N04_CHB.xLab21`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Peripheral Blood Sample analysis - Red Blood Cells" ;
create table  `your-project-id1234.N04_CHB.xLab22`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Peripheral Blood Sample analysis - White Blood Cells" ;
create table  `your-project-id1234.N04_CHB.xLab23`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Peripheral Blood Sample analysis - Whole" ;
create table  `your-project-id1234.N04_CHB.xLab24`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Pleural Sample analysis - Red Blood Cells" ;
create table  `your-project-id1234.N04_CHB.xLab25`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Pleural Sample analysis - White Blood Cells" ;
create table  `your-project-id1234.N04_CHB.xLab26`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Pleural Sample analysis - Whole" ;
create table  `your-project-id1234.N04_CHB.xLab27`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Respiratory Support" ;
create table  `your-project-id1234.N04_CHB.xLab28`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="Stool sample analysis - whole" ;
create table  `your-project-id1234.N04_CHB.xLab29`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="urine sample (24-hour) analysis" ;
create table  `your-project-id1234.N04_CHB.xLab30`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="urine sample (24-hour) Urinalysis" ;
create table  `your-project-id1234.N04_CHB.xLab31`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="urine sample (random) Culture test" ;
create table  `your-project-id1234.N04_CHB.xLab32`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="urine sample (random) Microscopic Examination" ;
create table  `your-project-id1234.N04_CHB.xLab33`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="urine sample (random) Substances Measurement" ;
create table  `your-project-id1234.N04_CHB.xLab34`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="urine sample (random) Urinalysis" ;
create table  `your-project-id1234.N04_CHB.xLab35`  as  SELECT * FROM `your-project-id1234.N04_CHB.e_Lab`  where fluid="urine sample (random) Urine Protein Electrophoresis" ;
'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
