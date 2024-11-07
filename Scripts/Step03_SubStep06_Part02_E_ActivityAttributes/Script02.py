import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



CREATE TABLE `your-project-id1234.I04_actPro.TabA1`  AS


    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M02_HA.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M04_DFH.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M05_DIH.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M06_ACU.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M07_TIO.TabD14`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M08_MBS.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M09_MTR.TabA14`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M10_MED.TabA15`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M11_TBO.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M12_IVT.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M13_CLT.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M15_RCC.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M16_DCP.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M17_DSO.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M18_TFO.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M19_ADO.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M20_WMS.TabD10`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M21_HMS.TabD10`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M22_EGFR.TabA10`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M23_BMI.TabA10`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M24_BP.TabA10`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M25_ABH.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M26_ABP.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M27_ABW.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M28_ASR.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M29_ASW.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M30_ASA.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M31_BMN.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M32_BMP.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M33_BMW.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M34_BMH.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M35_CSW.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M36_CSH.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M37_JFW.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M38_JFH.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M39_LAB.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M40_OBF.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M41_PBC.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M42_PBH.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M43_PBL.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M44_PBP.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M45_PBR.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M46_PBW.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M47_PBQ.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M48_PLR.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M49_PLW.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M50_PLH.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M51_RSP.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M52_STW.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M53_U24.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M54_UA4.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M55_UCR.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M56_UME.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M57_USM.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M58_URA.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M59_UPE.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M60_GVASO.TabA14`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M61_DBC.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M62_CBM.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M63_CLC.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M64_CAG.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M65_CBC.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M66_CRRT.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M67_DDB.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M68_ENZ.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M69_ICP.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M70_CRP.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M71_ILI.TabA14`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M72_OXD.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M73_CRI.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M74_SAPSII.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M75_NBM.TabA14`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M76_KDIGO.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M77_VASO.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M78_FDL.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M79_BGT.TabA10`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M80_FBGT.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M81_GCS.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M82_FGCS.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M83_RRT.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M84_VTN.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M85_FVTS.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M86_URD.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M87_URO.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M88_VTS.TabA14`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M89_VTR.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M90_ANT.TabA16`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M91_FSOFA.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M92_SEP.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M93_SOFA.TabA14`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M94_ABA.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M95_CUL.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Properties_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M96_ISP.TabA11`   ;

    
update  `your-project-id1234.I04_actPro.TabA1` 
set Activity="Microbiology_Blood_Sample"
where Activity="Mirobiology_Blood_Sample";


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
