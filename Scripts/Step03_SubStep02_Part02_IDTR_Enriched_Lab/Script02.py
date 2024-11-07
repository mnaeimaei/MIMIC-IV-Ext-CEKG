import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        CREATE TABLE  `your-project-id1234.N07_LAB.first_day_lab_Time`   AS
        SELECT distinct a.subject_id , a.stay_id , a.Timestamps, a.hematocrit_min , a.hematocrit_max , a.hemoglobin_min , a.hemoglobin_max , a.platelets_min , a.platelets_max , a.wbc_min , a.wbc_max , a.albumin_min , a.albumin_max , a.globulin_min , a.globulin_max , a.total_protein_min , a.total_protein_max , a.aniongap_min , a.aniongap_max , a.bicarbonate_min , a.bicarbonate_max , a.bun_min , a.bun_max , a.calcium_min , a.calcium_max , a.chloride_min , a.chloride_max , a.creatinine_min , a.creatinine_max , a.glucose_min , a.glucose_max , a.sodium_min , a.sodium_max , a.potassium_min , a.potassium_max , a.abs_basophils_min , a.abs_basophils_max , a.abs_eosinophils_min , a.abs_eosinophils_max , a.abs_lymphocytes_min , a.abs_lymphocytes_max , a.abs_monocytes_min , a.abs_monocytes_max , a.abs_neutrophils_min , a.abs_neutrophils_max , a.atyps_min , a.atyps_max , a.bands_min , a.bands_max , a.imm_granulocytes_min , a.imm_granulocytes_max , a.metas_min , a.metas_max , a.nrbc_min , a.nrbc_max , a.d_dimer_min , a.d_dimer_max , a.fibrinogen_min , a.fibrinogen_max , a.thrombin_min , a.thrombin_max , a.inr_min , a.inr_max , a.pt_min , a.pt_max , a.ptt_min , a.ptt_max , a.alt_min , a.alt_max , a.alp_min , a.alp_max , a.ast_min , a.ast_max , a.amylase_min , a.amylase_max , a.bilirubin_total_min , a.bilirubin_total_max , a.bilirubin_direct_min , a.bilirubin_direct_max , a.bilirubin_indirect_min , a.bilirubin_indirect_max , a.ck_cpk_min , a.ck_cpk_max , a.ck_mb_min , a.ck_mb_max , a.ggt_min , a.ggt_max , a.ld_ldh_min , a.ld_ldh_max FROM (
        SELECT
        a.subject_id , a.stay_id , a.hematocrit_min , a.hematocrit_max , a.hemoglobin_min , a.hemoglobin_max , a.platelets_min , a.platelets_max , a.wbc_min , a.wbc_max , a.albumin_min , a.albumin_max , a.globulin_min , a.globulin_max , a.total_protein_min , a.total_protein_max , a.aniongap_min , a.aniongap_max , a.bicarbonate_min , a.bicarbonate_max , a.bun_min , a.bun_max , a.calcium_min , a.calcium_max , a.chloride_min , a.chloride_max , a.creatinine_min , a.creatinine_max , a.glucose_min , a.glucose_max , a.sodium_min , a.sodium_max , a.potassium_min , a.potassium_max , a.abs_basophils_min , a.abs_basophils_max , a.abs_eosinophils_min , a.abs_eosinophils_max , a.abs_lymphocytes_min , a.abs_lymphocytes_max , a.abs_monocytes_min , a.abs_monocytes_max , a.abs_neutrophils_min , a.abs_neutrophils_max , a.atyps_min , a.atyps_max , a.bands_min , a.bands_max , a.imm_granulocytes_min , a.imm_granulocytes_max , a.metas_min , a.metas_max , a.nrbc_min , a.nrbc_max , a.d_dimer_min , a.d_dimer_max , a.fibrinogen_min , a.fibrinogen_max , a.thrombin_min , a.thrombin_max , a.inr_min , a.inr_max , a.pt_min , a.pt_max , a.ptt_min , a.ptt_max , a.alt_min , a.alt_max , a.alp_min , a.alp_max , a.ast_min , a.ast_max , a.amylase_min , a.amylase_max , a.bilirubin_total_min , a.bilirubin_total_max , a.bilirubin_direct_min , a.bilirubin_direct_max , a.bilirubin_indirect_min , a.bilirubin_indirect_max , a.ck_cpk_min , a.ck_cpk_max , a.ck_mb_min , a.ck_mb_max , a.ggt_min , a.ggt_max , a.ld_ldh_min , a.ld_ldh_max,
        b.careunit , b.min , b.max,
        DATETIME_ADD(min, INTERVAL 1 DAY) as Timestamps
        From `your-project-id1234.N07_LAB.first_day_lab`   as a
        LEFT JOIN `your-project-id1234.R_TimeC.SI`   as b
        ON
        a.subject_id=b.subject_id AND a.stay_id=b.stay_id 


        )  a  




'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
