import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

    CREATE TABLE  `your-project-id1234.N01_NDC.t3`  as 
    SELECT subject_id , hadm_id , pharmacy_id , poe_id , emar_id , emar_seq , poe_seq , order_provider_id , enter_provider_id , starttime , stoptime , charttime , scheduletime , storetime , event_txt , parent_field_ordinal , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc , product_code , prod_strength , form_val_disp , form_unit_disp , form_rx , drug_type , route , administration_type , dose_val_rx , dose_unit_rx , frequency , basal_rate , one_hr_max , doses_per_24_hrs , duration , duration_interval 
    FROM `your-project-id1234.N01_NDC.t2`  ;
 
'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
