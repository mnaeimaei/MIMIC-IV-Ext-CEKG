import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



CREATE TABLE `your-project-id1234.I17_DK7.TabA_features`  AS


    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M02_HA.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M04_DFH.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M05_DIH.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M06_ACU.TabA14`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M07_TIO.TabD15`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M08_MBS.TabA14`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M09_MTR.TabA15`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M10_MED.TabA16`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M11_TBO.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M12_IVT.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M13_CLT.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M15_RCC.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M16_DCP.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M17_DSO.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M18_TFO.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M19_ADO.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M20_WMS.TabD11`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M21_HMS.TabD11`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M22_EGFR.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M23_BMI.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M24_BP.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M25_ABH.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M26_ABP.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M27_ABW.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M28_ASR.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M29_ASW.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M30_ASA.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M31_BMN.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M32_BMP.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M33_BMW.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M34_BMH.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M35_CSW.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M36_CSH.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M37_JFW.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M38_JFH.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M39_LAB.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M40_OBF.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M41_PBC.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M42_PBH.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M43_PBL.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M44_PBP.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M45_PBR.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M46_PBW.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M47_PBQ.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M48_PLR.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M49_PLW.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M50_PLH.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M51_RSP.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M52_STW.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M53_U24.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M54_UA4.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M55_UCR.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M56_UME.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M57_USM.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M58_URA.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M59_UPE.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M60_GVASO.TabA15`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M61_DBC.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M62_CBM.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M63_CLC.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M64_CAG.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M65_CBC.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M66_CRRT.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M67_DDB.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M68_ENZ.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M69_ICP.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M70_CRP.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M71_ILI.TabA15`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M72_OXD.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M73_CRI.TabA14`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M74_SAPSII.TabA14`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M75_NBM.TabA15`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M76_KDIGO.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M77_VASO.TabA14`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M78_FDL.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M79_BGT.TabA11`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M80_FBGT.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M81_GCS.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M82_FGCS.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M83_RRT.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M84_VTN.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M85_FVTS.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M86_URD.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M87_URO.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M88_VTS.TabA15`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M89_VTR.TabA13`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M90_ANT.TabA17`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M91_FSOFA.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M92_SEP.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M93_SOFA.TabA15`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M94_ABA.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M95_CUL.TabA12`   
    union distinct  
    

    SELECT  
    Activity_Value_ID, Activity, Activity_Synonym, featureName, featureValue
    From `your-project-id1234.M96_ISP.TabA12`  ;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
