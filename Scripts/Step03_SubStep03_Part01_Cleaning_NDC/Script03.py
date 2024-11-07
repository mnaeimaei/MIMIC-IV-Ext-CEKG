import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

    CREATE TABLE  `your-project-id1234.N01_NDC.e2`  as 
    SELECT subject_id , hadm_id , emar_id , emar_seq , poe_id , pharmacy_id , enter_provider_id , charttime , medication as medication_e , event_txt , scheduletime , storetime , parent_field_ordinal , administration_type , barcode_type , reason_for_no_barcode , complete_dose_not_given , dose_due , dose_due_unit , dose_given , dose_given_unit , will_remainder_of_dose_be_given , product_amount_given , product_unit , product_code , product_description , product_description_other , prior_infusion_rate , infusion_rate , infusion_rate_adjustment , infusion_rate_adjustment_amount , infusion_rate_unit , route as route_e , infusion_complete , completion_interval , new_iv_bag_hung , continued_infusion_in_other_location , restart_interval , side , site , non_formulary_visual_verification 
    FROM `your-project-id1234.N01_NDC.e1` ;
    
    
    CREATE TABLE  `your-project-id1234.N01_NDC.m2`  as 
    SELECT subject_id , hadm_id , pharmacy_id , poe_id , poe_seq , order_provider_id ,starttime as starttime_m ,  stoptime as stoptime_m , drug_type , drug , formulary_drug_cd , gsn , ndc , prod_strength , form_rx , dose_val_rx , dose_unit_rx , form_val_disp , form_unit_disp , doses_per_24_hrs as doses_per_24_hrs_m , route as route_m 
    FROM `your-project-id1234.N01_NDC.m1` ;


        
    CREATE TABLE  `your-project-id1234.N01_NDC.p2`  as 
    SELECT subject_id , hadm_id , pharmacy_id , poe_id , starttime as starttime_p , stoptime as  stoptime_p , medication as medication_p , proc_type , status , entertime , verifiedtime , route as  route_p , frequency , disp_sched , infusion_type , sliding_scale , lockout_interval , basal_rate , one_hr_max , doses_per_24_hrs as doses_per_24_hrs_p , duration , duration_interval , expiration_value , expiration_unit , expirationdate , dispensation , fill_quantity 
    FROM `your-project-id1234.N01_NDC.p1` ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
