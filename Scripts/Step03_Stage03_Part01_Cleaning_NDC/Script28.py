import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE  `your-project-id1234.N01_NDC.x2`  as 
SELECT Ndc1 as NDC , Product as  Product_NDC, Package as Package_NDC, Package_11_digit as Package_NDC_11_digit, Proprietary_Name , Non_Proprietary_Name , activeIngredient__hippa as activeIngredient_hippa , Package_Description___ as Package_Description , Dosage_Form_Name , Route_Name , Pharmaceutical_Classes 
FROM `your-project-id1234.N01_NDC.x1`  ;


UPDATE  `your-project-id1234.N01_NDC.x2`
SET Ndc = REPLACE(Ndc, 'A', '')
WHERE Ndc is not null;


CREATE TABLE `your-project-id1234.N01_NDC.x3`  AS

SELECT
a.NDC, a.Product_NDC, a.Package_NDC, a.Package_NDC_11_digit, a.Proprietary_Name, a.Non_Proprietary_Name, a.activeIngredient_hippa, a.Package_Description, a.Dosage_Form_Name, a.Route_Name, a.Pharmaceutical_Classes ,
b.Dup as Proprietary_Name_rep
FROM `your-project-id1234.N01_NDC.x2`  a
LEFT   JOIN (
SELECT  
Proprietary_Name, COUNT(*) dup
FROM `your-project-id1234.N01_NDC.x2` 
GROUP BY Proprietary_Name) b
on 
a.Proprietary_Name = b.Proprietary_Name

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
