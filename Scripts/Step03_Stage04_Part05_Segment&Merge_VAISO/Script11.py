import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE  `your-project-id1234.N06_VASO.TabA61`   AS
SELECT distinct * FROM (
SELECT
b.subject_id , b.hadm_id , a.stay_id , b.orderid as solutionID, a.linkorderid , 
b.label as drug , b.amount as drug_amount ,  b.amountuom as drug_amount_uom , b.originalrate as original_drug_rate , b.rate as drug_rate , b.rateuom as drug_rate_uom ,   b.statusdescription as status_description ,
a.starttime , a.endtime , a.dobutamine_vaso_rate , a.dobutamine_vaso_amount , a.dopamine_vaso_rate , a.dopamine_vaso_amount , a.epinephrine_vaso_rate , a.epinephrine_vaso_amount , a.milrinone_vaso_rate , a.milrinone_vaso_amount , a.norepinephrine_vaso_rate , a.norepinephrine_vaso_amount , a.norepinephrine_equivalent_dose , a.phenylephrine_vaso_rate , a.phenylephrine_vaso_amount , a.vasopressin_vaso_rate , a.vasopressin_vaso_amount,

From `your-project-id1234.N06_VASO.TabA51`   as a
LEFT JOIN `your-project-id1234.S_inputEvents.1_inputEvent`   as b
ON
a.linkorderid=b.linkorderid )    
; 

############################################################################



CREATE TABLE  `your-project-id1234.N06_VASO.TabA62`   AS
SELECT distinct 
stay_id, starttime, endtime, dobutamine_vaso_rate, dobutamine_vaso_amount, dopamine_vaso_rate, dopamine_vaso_amount, epinephrine_vaso_rate, epinephrine_vaso_amount, milrinone_vaso_rate, milrinone_vaso_amount, norepinephrine_vaso_rate, norepinephrine_vaso_amount, norepinephrine_equivalent_dose, phenylephrine_vaso_rate, phenylephrine_vaso_amount, vasopressin_vaso_rate, vasopressin_vaso_amount

From `your-project-id1234.N06_VASO.TabA52` 


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
