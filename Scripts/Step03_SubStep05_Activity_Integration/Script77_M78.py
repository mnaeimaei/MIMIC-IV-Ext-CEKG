import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M78_FDL` ;
create table `your-project-id1234.M78_FDL.TabA1` as
SELECT  * FROM `your-project-id1234.N07_LAB.first_day_lab_Time` ;



CREATE TABLE  `your-project-id1234.M78_FDL.TabA2`   AS
SELECT distinct * FROM (
SELECT
a.subject_id , b.hadm_id,a.stay_id , a.Timestamps , a.hematocrit_min , a.hematocrit_max , a.hemoglobin_min , a.hemoglobin_max , a.platelets_min , a.platelets_max , a.wbc_min , a.wbc_max , a.albumin_min , a.albumin_max , a.globulin_min , a.globulin_max , a.total_protein_min , a.total_protein_max , a.aniongap_min , a.aniongap_max , a.bicarbonate_min , a.bicarbonate_max , a.bun_min , a.bun_max , a.calcium_min , a.calcium_max , a.chloride_min , a.chloride_max , a.creatinine_min , a.creatinine_max , a.glucose_min , a.glucose_max , a.sodium_min , a.sodium_max , a.potassium_min , a.potassium_max , a.abs_basophils_min , a.abs_basophils_max , a.abs_eosinophils_min , a.abs_eosinophils_max , a.abs_lymphocytes_min , a.abs_lymphocytes_max , a.abs_monocytes_min , a.abs_monocytes_max , a.abs_neutrophils_min , a.abs_neutrophils_max , a.atyps_min , a.atyps_max , a.bands_min , a.bands_max , a.imm_granulocytes_min , a.imm_granulocytes_max , a.metas_min , a.metas_max , a.nrbc_min , a.nrbc_max , a.d_dimer_min , a.d_dimer_max , a.fibrinogen_min , a.fibrinogen_max , a.thrombin_min , a.thrombin_max , a.inr_min , a.inr_max , a.pt_min , a.pt_max , a.ptt_min , a.ptt_max , a.alt_min , a.alt_max , a.alp_min , a.alp_max , a.ast_min , a.ast_max , a.amylase_min , a.amylase_max , a.bilirubin_total_min , a.bilirubin_total_max , a.bilirubin_direct_min , a.bilirubin_direct_max , a.bilirubin_indirect_min , a.bilirubin_indirect_max , a.ck_cpk_min , a.ck_cpk_max , a.ck_mb_min , a.ck_mb_max , a.ggt_min , a.ggt_max , a.ld_ldh_min , a.ld_ldh_max,
From `your-project-id1234.M78_FDL.TabA1`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SHI`   as b
ON
a.subject_id=b.subject_id AND a.stay_id=b.stay_id
AND  a.Timestamps>=b.min AND a.Timestamps<=b.max
)
;



CREATE TABLE  `your-project-id1234.M78_FDL.TabA3`   AS
SELECT distinct   
subject_id, hadm_id, Timestamps, hematocrit_min, hematocrit_max, hemoglobin_min, hemoglobin_max, platelets_min, platelets_max, wbc_min, wbc_max, albumin_min, albumin_max, globulin_min, globulin_max, total_protein_min, total_protein_max, aniongap_min, aniongap_max, bicarbonate_min, bicarbonate_max, bun_min, bun_max, calcium_min, calcium_max, chloride_min, chloride_max, creatinine_min, creatinine_max, glucose_min, glucose_max, sodium_min, sodium_max, potassium_min, potassium_max, abs_basophils_min, abs_basophils_max, abs_eosinophils_min, abs_eosinophils_max, abs_lymphocytes_min, abs_lymphocytes_max, abs_monocytes_min, abs_monocytes_max, abs_neutrophils_min, abs_neutrophils_max, atyps_min, atyps_max, bands_min, bands_max, imm_granulocytes_min, imm_granulocytes_max, metas_min, metas_max, nrbc_min, nrbc_max, d_dimer_min, d_dimer_max, fibrinogen_min, fibrinogen_max, thrombin_min, thrombin_max, inr_min, inr_max, pt_min, pt_max, ptt_min, ptt_max, alt_min, alt_max, alp_min, alp_max, ast_min, ast_max, amylase_min, amylase_max, bilirubin_total_min, bilirubin_total_max, bilirubin_direct_min, bilirubin_direct_max, bilirubin_indirect_min, bilirubin_indirect_max, ck_cpk_min, ck_cpk_max, ck_mb_min, ck_mb_max, ggt_min, ggt_max, ld_ldh_min, ld_ldh_max  
From `your-project-id1234.M78_FDL.TabA2` ;



ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M78_FDL.TabA3`
SET Activity_Synonym ="FDL"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M78_FDL.TabA3`
SET Activity ="First_day_lab_Test"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f1 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f1="hematocrit_min" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f2 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f2="hematocrit_max" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f3 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f3="hemoglobin_min" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f4 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f4="hemoglobin_max" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f5 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f5="platelets_min" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f6 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f6="platelets_max" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f7 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f7="wbc_min" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f8 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f8="wbc_max" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f9 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f9="albumin_min" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f10 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f10="albumin_max" 
where f10 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f11 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f11="globulin_min" 
where f11 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f12 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f12="globulin_max" 
where f12 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f13 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f13="total_protein_min" 
where f13 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f14 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f14 ="total_protein_max" 
where f14 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f15 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f15 ="aniongap_min" 
where f15 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f16 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f16 ="aniongap_max" 
where f16 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f17 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f17 ="bicarbonate_min" 
where f17 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f18 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f18 ="bicarbonate_max" 
where f18 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f19 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f19 ="bun_min" 
where f19 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f20 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f20 ="bun_max" 
where f20 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f21 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f21 ="calcium_min" 
where f21 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f22 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f22 ="calcium_max" 
where f22 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f23 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f23 ="chloride_min" 
where f23 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f24 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f24 ="chloride_max" 
where f24 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f25 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f25 ="creatinine_min" 
where f25 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f26 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f26="creatinine_max" 
where f26 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f27 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f27="glucose_min" 
where f27 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f28 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f28="glucose_max" 
where f28 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f29 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f29="sodium_min" 
where f29 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f30 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f30="sodium_max" 
where f30 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f31 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f31="potassium_min" 
where f31 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f32 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f32="potassium_max" 
where f32 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f33 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f33="abs_basophils_min" 
where f33 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f34 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f34="abs_basophils_max" 
where f34 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f35 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f35="abs_eosinophils_min" 
where f35 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f36 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f36="abs_eosinophils_max" 
where f36 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f37 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f37="abs_lymphocytes_min" 
where f37 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f38 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f38="abs_lymphocytes_max" 
where f38 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f39 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f39 ="abs_monocytes_min" 
where f39 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f40 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f40 ="abs_monocytes_max" 
where f40 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f41 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f41 ="abs_neutrophils_min" 
where f41 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f42 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f42 ="abs_neutrophils_max" 
where f42 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f43 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f43 ="atyps_min" 
where f43 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f44 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f44 ="atyps_max" 
where f44 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f45 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f45 ="bands_min" 
where f45 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f46 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f46 ="bands_max" 
where f46 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f47 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f47 ="imm_granulocytes_min" 
where f47 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f48 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f48 ="imm_granulocytes_max" 
where f48 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f49 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f49 ="metas_min" 
where f49 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f50 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f50 ="metas_max" 
where f50 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f51 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f51="nrbc_min" 
where f51 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f52 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f52="nrbc_max" 
where f52 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f53 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f53="d_dimer_min" 
where f53 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f54 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f54="d_dimer_max" 
where f54 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f55 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f55="fibrinogen_min" 
where f55 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f56 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f56="fibrinogen_max" 
where f56 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f57 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f57="thrombin_min" 
where f57 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f58 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f58="thrombin_max" 
where f58 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f59 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f59="inr_min" 
where f59 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f60 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f60="inr_max" 
where f60 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f61 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f61="pt_min" 
where f61 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f62 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f62="pt_max" 
where f62 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f63 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f63="ptt_min" 
where f63 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f64 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f64 ="ptt_max" 
where f64 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f65 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f65 ="alt_min" 
where f65 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f66 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f66 ="alt_max" 
where f66 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f67 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f67 ="alp_min" 
where f67 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f68 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f68 ="alp_max" 
where f68 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f69 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f69 ="ast_min" 
where f69 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f70 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f70 ="ast_max" 
where f70 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f71 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f71 ="amylase_min" 
where f71 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f72 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f72 ="amylase_max" 
where f72 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f73 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f73 ="bilirubin_total_min" 
where f73 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f74 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f74 ="bilirubin_total_max" 
where f74 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f75 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f75 ="bilirubin_direct_min" 
where f75 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f76 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f76="bilirubin_direct_max" 
where f76 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f77 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f77="bilirubin_indirect_min" 
where f77 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f78 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f78="bilirubin_indirect_max" 
where f78 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f79 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f79="ck_cpk_min" 
where f79 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f80 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f80="ck_cpk_max" 
where f80 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f81 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f81="ck_mb_min" 
where f81 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f82 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f82="ck_mb_max" 
where f82 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f83 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f83="ggt_min" 
where f83 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f84 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f84="ggt_max" 
where f84 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f85 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f85="ld_ldh_min" 
where f85 is null;
#################################################
ALTER TABLE `your-project-id1234.M78_FDL.TabA3`
ADD column  f86 STRING ;
update `your-project-id1234.M78_FDL.TabA3`
set f86="ld_ldh_max" 
where f86 is null;
#################################################
################################################################

create table `your-project-id1234.M78_FDL.TabA4`   as
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(hematocrit_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(hematocrit_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(hemoglobin_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(hemoglobin_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(platelets_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(platelets_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(wbc_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(wbc_max AS STRING) as  value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(albumin_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(albumin_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f11 as feature , CAST(globulin_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f12 as feature , CAST(globulin_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f13 as feature , CAST(total_protein_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f14 as feature , CAST(total_protein_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f15 as feature , CAST(aniongap_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f16 as feature , CAST(aniongap_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f17 as feature , CAST(bicarbonate_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f18 as feature , CAST(bicarbonate_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f19 as feature , CAST(bun_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f20 as feature , CAST(bun_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f21 as feature , CAST(calcium_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f22 as feature , CAST(calcium_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f23 as feature , CAST(chloride_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f24 as feature , CAST(chloride_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f25 as feature , CAST(creatinine_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f26 as feature , CAST(creatinine_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f27 as feature , CAST(glucose_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f28 as feature , CAST(glucose_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f29 as feature , CAST(sodium_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f30 as feature , CAST(sodium_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f31 as feature , CAST(potassium_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f32 as feature , CAST(potassium_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f33 as feature , CAST(abs_basophils_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f34 as feature , CAST(abs_basophils_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f35 as feature , CAST(abs_eosinophils_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f36 as feature , CAST(abs_eosinophils_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f37 as feature , CAST(abs_lymphocytes_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f38 as feature , CAST(abs_lymphocytes_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f39 as feature , CAST(abs_monocytes_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f40 as feature , CAST(abs_monocytes_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f41 as feature , CAST(abs_neutrophils_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f42 as feature , CAST(abs_neutrophils_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f43 as feature , CAST(atyps_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f44 as feature , CAST(atyps_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f45 as feature , CAST(bands_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f46 as feature , CAST(bands_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f47 as feature , CAST(imm_granulocytes_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f48 as feature , CAST(imm_granulocytes_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f49 as feature , CAST(metas_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f50 as feature , CAST(metas_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f51 as feature , CAST(nrbc_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f52 as feature , CAST(nrbc_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f53 as feature , CAST(d_dimer_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f54 as feature , CAST(d_dimer_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f55 as feature , CAST(fibrinogen_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f56 as feature , CAST(fibrinogen_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f57 as feature , CAST(thrombin_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f58 as feature , CAST(thrombin_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f59 as feature , CAST(inr_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f60 as feature , CAST(inr_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f61 as feature , CAST(pt_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f62 as feature , CAST(pt_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f63 as feature , CAST(ptt_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f64 as feature , CAST(ptt_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f65 as feature , CAST(alt_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f66 as feature , CAST(alt_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f67 as feature , CAST(alp_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f68 as feature , CAST(alp_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f69 as feature , CAST(ast_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f70 as feature , CAST(ast_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f71 as feature , CAST(amylase_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f72 as feature , CAST(amylase_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f73 as feature , CAST(bilirubin_total_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f74 as feature , CAST(bilirubin_total_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f75 as feature , CAST(bilirubin_direct_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f76 as feature , CAST(bilirubin_direct_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f77 as feature , CAST(bilirubin_indirect_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f78 as feature , CAST(bilirubin_indirect_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f79 as feature , CAST(ck_cpk_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f80 as feature , CAST(ck_cpk_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f81 as feature , CAST(ck_mb_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f82 as feature , CAST(ck_mb_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f83 as feature , CAST(ggt_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f84 as feature , CAST(ggt_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f85 as feature , CAST(ld_ldh_min AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f86 as feature , CAST(ld_ldh_max AS STRING) as value
from  `your-project-id1234.M78_FDL.TabA3`  ;
#################################################
alter table `your-project-id1234.M78_FDL.TabA4`
add column num INT64;
update`your-project-id1234.M78_FDL.TabA4`
set num=1
where num is null;




CREATE TABLE `your-project-id1234.M78_FDL.TabA5`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M78_FDL.TabA4`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M78_FDL.TabA4`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;





ALTER TABLE `your-project-id1234.M78_FDL.TabA5`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M78_FDL.TabA5`
SET Activity_Value_ID = concat("fdl",RankN)
WHERE Activity_Value_ID is null ;



CREATE TABLE `your-project-id1234.M78_FDL.TabA6`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M78_FDL.TabA5`  ;

CREATE TABLE `your-project-id1234.M78_FDL.TabA7`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M78_FDL.TabA5`  ;



ALTER TABLE `your-project-id1234.M78_FDL.TabA7`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M78_FDL.TabA7`
SET Activity_Synonym = "FDL"
WHERE Activity_Synonym is null ;


ALTER TABLE `your-project-id1234.M78_FDL.TabA7`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M78_FDL.TabA7`
SET num = 1
WHERE num is null ;




CREATE TABLE `your-project-id1234.M78_FDL.TabA8`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M78_FDL.TabA7`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M78_FDL.TabA7`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M78_FDL.TabA9`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M78_FDL.TabA8`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M78_FDL.TabA10`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M78_FDL.TabA9`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M78_FDL.TabA11`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M78_FDL.TabA6`   as a
LEFT JOIN `your-project-id1234.M78_FDL.TabA10`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M78_FDL.TabA12`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M78_FDL.TabA8`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M78_FDL.TabA13`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M78_FDL.TabA7`  )    ;



CREATE TABLE `your-project-id1234.M78_FDL.TabA14`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M78_FDL.TabA9`    as a
LEFT JOIN   `your-project-id1234.M78_FDL.TabA10`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;






'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
