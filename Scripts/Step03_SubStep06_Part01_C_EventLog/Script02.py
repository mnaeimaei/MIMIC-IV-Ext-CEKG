import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            




CREATE TABLE `your-project-id1234.I01_Log.TabA1`  AS


    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M01_RFE.TabA3`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M02_HA.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M03_DFE.TabA3`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M04_DFH.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M05_DIH.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M06_ACU.TabA12`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M07_TIO.TabD13`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M08_MBS.TabA12`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M09_MTR.TabA13`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M10_MED.TabA14`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M11_TBO.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M12_IVT.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M13_CLT.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M15_RCC.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M16_DCP.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M17_DSO.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M18_TFO.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M19_ADO.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M20_WMS.TabD9`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M21_HMS.TabD9`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M22_EGFR.TabA9`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M23_BMI.TabA9`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M24_BP.TabA9`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M25_ABH.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M26_ABP.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M27_ABW.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M28_ASR.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M29_ASW.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M30_ASA.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M31_BMN.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M32_BMP.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M33_BMW.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M34_BMH.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M35_CSW.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M36_CSH.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M37_JFW.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M38_JFH.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M39_LAB.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M40_OBF.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M41_PBC.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M42_PBH.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M43_PBL.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M44_PBP.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M45_PBR.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M46_PBW.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M47_PBQ.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M48_PLR.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M49_PLW.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M50_PLH.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M51_RSP.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M52_STW.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M53_U24.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M54_UA4.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M55_UCR.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M56_UME.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M57_USM.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M58_URA.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M59_UPE.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M60_GVASO.TabA13`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M61_DBC.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M62_CBM.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M63_CLC.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M64_CAG.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M65_CBC.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M66_CRRT.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M67_DDB.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M68_ENZ.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M69_ICP.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M70_CRP.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M71_ILI.TabA13`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M72_OXD.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M73_CRI.TabA12`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M74_SAPSII.TabA12`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M75_NBM.TabA13`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M76_KDIGO.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M77_VASO.TabA12`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M78_FDL.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M79_BGT.TabA9`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M80_FBGT.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M81_GCS.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M82_FGCS.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M83_RRT.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M84_VTN.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M85_FVTS.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M86_URD.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M87_URO.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M88_VTS.TabA13`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M89_VTR.TabA11`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M90_ANT.TabA15`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M91_FSOFA.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M92_SEP.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M93_SOFA.TabA13`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M94_ABA.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M95_CUL.TabA10`   
    union distinct  
    

    SELECT  
    subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
    From `your-project-id1234.M96_ISP.TabA10`   ;

update  `your-project-id1234.I01_Log.TabA1` 
set Activity="Microbiology_Blood_Sample"
where Activity="Mirobiology_Blood_Sample";
        

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
