import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.N01_NDC.zzzzt5`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.subject_id , a.hadm_id , a.pharmacy_id , a.poe_id , a.emar_id , a.emar_seq , a.poe_seq , a.order_provider_id , a.enter_provider_id , a.starttime , a.stoptime , a.charttime , a.scheduletime , a.storetime , a.event_txt , a.parent_field_ordinal , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , b.Ndc as Ndc , a.product_code , a.prod_strength , a.form_val_disp , a.form_unit_disp , a.form_rx , a.drug_type , a.route , a.administration_type , a.dose_val_rx , a.dose_unit_rx , a.frequency , a.basal_rate , a.one_hr_max , a.doses_per_24_hrs , a.duration , a.duration_interval,
        From `your-project-id1234.N01_NDC.t4`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.zzzu1`   as b
        ON
        a.eventID=b.eventID )      ; 




'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
