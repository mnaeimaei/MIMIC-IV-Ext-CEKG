import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.N01_NDC.s2`   AS
        SELECT * FROM (
        SELECT
        a.subject_id , a.hadm_id , a.pharmacy_id , a.poe_id,
        b.emar_id , b.emar_seq , b.enter_provider_id , b.charttime , b.medication_e , b.event_txt , b.scheduletime , b.storetime , b.parent_field_ordinal , b.administration_type , b.barcode_type , b.reason_for_no_barcode , b.complete_dose_not_given , b.dose_due , b.dose_due_unit , b.dose_given , b.dose_given_unit , b.will_remainder_of_dose_be_given , b.product_amount_given , b.product_unit , b.product_code , b.product_description , b.product_description_other , b.prior_infusion_rate , b.infusion_rate , b.infusion_rate_adjustment , b.infusion_rate_adjustment_amount , b.infusion_rate_unit , b.route_e , b.infusion_complete , b.completion_interval , b.new_iv_bag_hung , b.continued_infusion_in_other_location , b.restart_interval , b.side , b.site , b.non_formulary_visual_verification,
        From `your-project-id1234.N01_NDC.s1`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.e2`   as b
        ON
        a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id AND a.pharmacy_id=b.pharmacy_id AND a.poe_id=b.poe_id )    
    ; 
    
        CREATE TABLE  `your-project-id1234.N01_NDC.s3`   AS
        SELECT * FROM (
        SELECT
        a.subject_id , a.hadm_id , a.pharmacy_id , a.poe_id , b.poe_seq , a.emar_id , a.emar_seq , a.enter_provider_id , a.charttime , a.medication_e , a.event_txt , a.scheduletime , a.storetime , a.parent_field_ordinal , a.administration_type , a.barcode_type , a.reason_for_no_barcode , a.complete_dose_not_given , a.dose_due , a.dose_due_unit , a.dose_given , a.dose_given_unit , a.will_remainder_of_dose_be_given , a.product_amount_given , a.product_unit , a.product_code , a.product_description , a.product_description_other , a.prior_infusion_rate , a.infusion_rate , a.infusion_rate_adjustment , a.infusion_rate_adjustment_amount , a.infusion_rate_unit , a.route_e , a.infusion_complete , a.completion_interval , a.new_iv_bag_hung , a.continued_infusion_in_other_location , a.restart_interval , a.side , a.site , a.non_formulary_visual_verification,
         b.order_provider_id , b.starttime_m , b.stoptime_m , b.drug_type , b.drug , b.formulary_drug_cd , b.gsn , b.ndc , b.prod_strength , b.form_rx , b.dose_val_rx , b.dose_unit_rx , b.form_val_disp , b.form_unit_disp , b.doses_per_24_hrs_m , b.route_m,
        From `your-project-id1234.N01_NDC.s2`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.m2`   as b
        ON
        a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id AND a.pharmacy_id=b.pharmacy_id AND a.poe_id=b.poe_id )    
    ; 
    
            CREATE TABLE  `your-project-id1234.N01_NDC.s4`   AS
        SELECT * FROM (
        SELECT
        a.subject_id , a.hadm_id , a.pharmacy_id , a.poe_id , a.poe_seq , a.emar_id , a.emar_seq , a.enter_provider_id , a.charttime , a.medication_e , a.event_txt , a.scheduletime , a.storetime , a.parent_field_ordinal , a.administration_type , a.barcode_type , a.reason_for_no_barcode , a.complete_dose_not_given , a.dose_due , a.dose_due_unit , a.dose_given , a.dose_given_unit , a.will_remainder_of_dose_be_given , a.product_amount_given , a.product_unit , a.product_code , a.product_description , a.product_description_other , a.prior_infusion_rate , a.infusion_rate , a.infusion_rate_adjustment , a.infusion_rate_adjustment_amount , a.infusion_rate_unit , a.route_e , a.infusion_complete , a.completion_interval , a.new_iv_bag_hung , a.continued_infusion_in_other_location , a.restart_interval , a.side , a.site , a.non_formulary_visual_verification , a.order_provider_id , a.starttime_m , a.stoptime_m , a.drug_type , a.drug , a.formulary_drug_cd , a.gsn , a.ndc , a.prod_strength , a.form_rx , a.dose_val_rx , a.dose_unit_rx , a.form_val_disp , a.form_unit_disp , a.doses_per_24_hrs_m , a.route_m,
        b.starttime_p , b.stoptime_p , b.medication_p , b.proc_type , b.status , b.entertime , b.verifiedtime , b.route_p , b.frequency , b.disp_sched , b.infusion_type , b.sliding_scale , b.lockout_interval , b.basal_rate , b.one_hr_max , b.doses_per_24_hrs_p , b.duration , b.duration_interval , b.expiration_value , b.expiration_unit , b.expirationdate , b.dispensation , b.fill_quantity,
        From `your-project-id1234.N01_NDC.s3`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.p2`   as b
        ON
        a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id AND a.pharmacy_id=b.pharmacy_id AND a.poe_id=b.poe_id )    
    ; 

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
