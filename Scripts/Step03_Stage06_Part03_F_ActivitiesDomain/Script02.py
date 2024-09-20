import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create table `your-project-id1234.K01_SCT.A_Domain`   as
select * from `your-project-id1234.K01_SCT.A_Activity` ;

alter table `your-project-id1234.K01_SCT.A_Domain` 
add column Domain string;

update  `your-project-id1234.K01_SCT.A_Domain` 
set Domain="Admissions_and_Transfers"
where Activity in ('Reg_from_ED', 'Hospital_Admission', 'Admission_to_Careunit', 'Transfer_Careunit', 'Admission_Order', 'Transfer_Order', 'Discharge_Order', 'Discharge_Planning');

update  `your-project-id1234.K01_SCT.A_Domain` 
set Domain="Diagnostics_and_Lab_test"
where Activity in  ('Microbiology_Blood_Sample', 'Microbiology_Test_Result', 'Lab_Test', 'First_day_lab_Test', 'Blood_Gas_Test', 'First_Day_Blood_Gas_Test', 'differential_blood_count', 'Cardiac_biomarkers', 'Clinical_chemistry', 'Coagulation', 'complete_blood_count', 'Detailed_differential_blood_count', 'Enzyme_level', 'C_Reactive_protein', 'First_Day_GCS_Assessment', 'GCS_Assessment');

update  `your-project-id1234.K01_SCT.A_Domain` 
set Domain="Sample_Analysis"
where Activity in  ('Arterial_blood_sample_analysis_hemoglobin', 'Arterial_blood_sample_analysis_plasma', 'Arterial_blood_sample_analysis_whole', 'Ascite_Sample_analysis_Red_Blood_Cells', 'Ascite_Sample_analysis_White_Blood_Cells', 'Ascite_Sample_analysis_Whole', 'Bone_Marrow_Sample_analysis_Nucleated_Cells', 'Bone_Marrow_Sample_analysis_Plasma', 'Bone_Marrow_Sample_analysis_White_Blood_Cells', 'Bone_Marrow_Sample_analysis_Whole', 'Cerebrospinal_Sample_analysis_White_Blood_Cells', 'Cerebrospinal_Sample_analysis_Whole', 'Joint_Fluid_sample_analysis_White_Blood_Cells', 'Joint_Fluid_sample_analysis_Whole', 'Peripheral_Blood_Sample_analysis_Creatine_Kinase', 'Peripheral_Blood_Sample_analysis_Hemoglobin', 'Peripheral_Blood_Sample_analysis_lymphocyte', 'Peripheral_Blood_Sample_analysis_Plasma', 'Peripheral_Blood_Sample_analysis_Red_Blood_Cells', 'Peripheral_Blood_Sample_analysis_White_Blood_Cells', 'Peripheral_Blood_Sample_analysis_Whole', 'Pleural_Sample_analysis_Red_Blood_Cells', 'Pleural_Sample_analysis_White_Blood_Cells', 'Pleural_Sample_analysis_Whole', 'Other_Body_Fluid_Analysis', 'Stool_sample_analysis_whole');

update  `your-project-id1234.K01_SCT.A_Domain` 
set Domain="Monitoring_and_Treatment"
where Activity in  ('Respiratory_Support', 'IV_therapy_order', 'Medication', 'Generic_Vasopressor_inotropic_medication_administration', 'Specific_Vasopressor_inotropic_medication_administration', 'Renal_Replacement_Therapy_Administration', 'Continuous_renal_replacement_therapy', 'Intracranial_Pressure', 'Invasive_line_insertion', 'oxygen_delivery', 'cardiac_rhythm_information', 'neurological_blockades_medications', 'Acute_Kidney_Injury_Monitoring', 'Urine_Output', 'Urine_Output_Detailed', 'vitalsign', 'First_day_vitalsign', 'ventilation_status', 'ventilator_setting', 'Antibiotic_Administration', 'antibiotic_administrations');

update  `your-project-id1234.K01_SCT.A_Domain` 
set Domain="Discharge_and_Outcomes"
where Activity in  ('Discharging_from_ED', 'Discharging_from_Hospital', 'Dying_in_hospital');

update  `your-project-id1234.K01_SCT.A_Domain` 
set Domain="Assessment_and_Planning"
where Activity  in  ('Consultation', 'Resuscitation_code', 'Sequential_Organ_Failure_Assessment', 'First_Day_Sequential_Organ_Failure_Assessment', 'Sepsis_3_Criteria_Assessment');

update  `your-project-id1234.K01_SCT.A_Domain` 
set Domain="Miscellaneous"
where Activity in  ('Tubing_Order', 'Weight_Measurement', 'Height_Measurement', 'Estimated_glomerular_filtration_rate', 'BMI_Calculation', 'BP_Measurement', 'Simplified Acute Physiology Scores', 'culture_results', 'infection_suspicions', 'urine_sample_24_hour_analysis', 'urine_sample_24_hour_Urinalysis', 'urine_sample_random_Culture_test', 'urine_sample_random_Microscopic_Examination', 'urine_sample_random_Substances_Measurement', 'urine_sample_random_Urinalysis', 'urine_sample_random_Urine_Protein_Electrophoresis');


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
