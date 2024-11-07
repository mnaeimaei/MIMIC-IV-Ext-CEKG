import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
#print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create schema `your-project-id1234.S_eMAR` ;

create table `your-project-id1234.S_eMAR.1_eMAR` as

        SELECT
        
a.subject_id, 
a.hadm_id, 
a.emar_id, 
a.emar_seq, 
a.poe_id, 
a.pharmacy_id, 
a.enter_provider_id, 
a.charttime, 
a.medication, 
a.event_txt, 
a.scheduletime, 
a.storetime,

b.parent_field_ordinal, 
b.administration_type, 
b.barcode_type, 
b.reason_for_no_barcode, 
b.complete_dose_not_given, 
b.dose_due, 
b.dose_due_unit, 
b.dose_given, 
b.dose_given_unit, 
b.will_remainder_of_dose_be_given, 
b.product_amount_given, 
b.product_unit, 
b.product_code, 
b.product_description, 
b.product_description_other, 
b.prior_infusion_rate, 
b.infusion_rate, 
b.infusion_rate_adjustment, 
b.infusion_rate_adjustment_amount, 
b.infusion_rate_unit, 
b.route, 
b.infusion_complete, 
b.completion_interval, 
b.new_iv_bag_hung, 
b.continued_infusion_in_other_location, 
b.restart_interval, 
b.side, 
b.site, 
b.non_formulary_visual_verification,

        From `your-project-id1234.x_mimiciv_hosp.emar`   as a
        LEFT JOIN `your-project-id1234.x_mimiciv_hosp.emar_detail`   as b
        ON
        a.emar_id=b.emar_id 

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
