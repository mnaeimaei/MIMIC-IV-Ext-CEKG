# Reproducibility of the MIMIC-IV-Ext-CEKG Dataset

This README file explains how to reproduce the MIMIC-IV-Ext-CEKG dataset as it is in the published version.

# Step1: Setting Up Infrastructure and Tools for Dataset Creation


## Stage 1: Creating a Google Cloud SQL Service Account
We used Google BigQuery (BQ) as the relational database management system (RDBMS) and executed queries through Python scripts. However, you can use other RDBMS platforms, though this will require adapting the queries to fit the specific system. If you wish to send queries from Python to Google BigQuery, a Google Cloud service key is required. Alternatively, if you prefer to run the queries directly in Google BigQuery, you can simply copy and paste the queries from the Python files to achieve the same results.

The following are the steps for creating a Google Cloud service account.

   - Log into your Google account  
   - Navigate to the Google Cloud Console, select your project or create a new one. Use the project dropdown at the top of the page to choose or create a new project.
   - Go to IAM & Admin > Service Accounts
   - Select a project again if necessary.
   - Click the `+ Create service account` button at the top of the page.
   - Service Account Name: Assign a name to your service account (can be anything).
   - Service Account ID (optional): The ID is auto-generated based on the name, but you can edit it if necessary.
   - Description: Optionally add a description of what the service account is for.
   - Click `Create and Continue`.
   - You need to assign one or more roles to it. Select `Basic/Owner` for the role.
   - Click `Continue`.
   - Click `Done`.
   - Again go to IAM & Admin > Service Accounts.
   - Select the same project again if necessary.
   - Click the email address of the service account for which you want to create a key.
   - Click the `Keys` tab.
   - Click the `Add key` drop-down menu, then select `Create new key`.
   - Select `JSON` as the key type and click `Create`. The key will download automatically.
   - Change the name of the key to "CEKG.json"

**Important**: After downloading your service account key, store it in a secure location on your computer.


## Stage 2: Create a Python Project

First, set up the Python project with a virtual environment. Once the virtual environment is activated, install the following packages using pip:

```bash
pip install google-cloud-bigquery
pip install pandas
pip install google-auth
pip install pandas-gbq
```


## Stage 3: Clone or Download the Repository and Organize the Files in the Python Project

Download or clone the repository. Ensure you download the **Scripts** directory.

Copy all directories/folders from the **Scripts** section and paste them into the root of your project.

Create a directory in the root of your project named `gcKey` and Place the downloaded and renamed JSON key file into this `gcKey` directory.

Below is a tree-like structure illustrating how to organize your files:

<img src="./README_resources/0_tree.png" alt="Alt text" width="300" height="1000"/>


## Stage 4: Change the Project ID

To update the project ID used in your Python project:

   - Visit the [Google Cloud Console](https://console.cloud.google.com/bigquery).
   - You can find your project ID listed in the left sidebar.
   - Navigate to `Step01_Stage04_changeProjID/Script1.py` within your python project directory.
   - Open `Script1.py` and locate the line `new_project_id = 'your-project-id1234'`.
   - Replace `'your-project-id1234'` with your actual Google Cloud project ID.


These steps ensure that the scripts within your project are correctly configured to interact with your specific Google Cloud environment.



# Step2: Preparation of Input Data

## Stage 1 - Part 1: Access to MIMIC-IV Modules

To work with the MIMIC-IV dataset, you will need to access various modules on Google Cloud BigQuery. Below are the links to each module and where to find the associated data:

  - Access data for the `mimiciv_hosp`, `mimiciv_icu`, and `mimiciv_derived` modules at [PhysioNet MIMIC-IV](https://physionet.org/content/mimiciv/).
  - For the `mimiciv_ED` module, visit [PhysioNet MIMIC-IV ED](https://physionet.org/content/mimic-iv-ed/).
  - The `mimiciv_note` module can be accessed at [PhysioNet MIMIC-IV Note](https://physionet.org/content/mimic-iv-note/).

Ensure you have the appropriate permissions and access granted by PhysioNet for utilizing these datasets.



## Stage 1 - Part 2: Copy MIMIC-IV Modules to Your Project on Google BigQuery

You will need to ensure that the MIMIC data modules are correctly transferred to your Google BigQuery environment, enabling you to work with them directly in your project. To transfer the necessary MIMIC modules to your Google BigQuery project:

   - Open your project directory and navigate to `Step02_Stage01_Part02_CopyDatabase/Query1.sql`.
   - Open `Query1.sql` and copy all the queries contained within the file.
   - Go to the Google BigQuery console.
   - Paste the copied queries into the query editor.
   - Run the queries to copy the required MIMIC modules to your project.


## Stage 1 - Part 3: Categorizing the Tables of the Dataset for Further Analysis

To facilitate easier analysis, we organize the dataset's tables into different categories by creating a new database structure. This is accomplished using the script located in `Step02_Stage01_Part03_Categorizing`. This reorganization involves various operations on the tables:

- **Renaming Tables**: Some tables are simply renamed and moved to the new database.
- **Splitting Tables**: For some tables, we split the data into different columns based on specific criteria, creating segmented tables that focus on particular aspects.
- **Joining Tables**: We perform left joins on certain tables, merging them based on relevant columns, then transfer the resulting new table to the newly created database.

The details of each operation, including the specific scripts used and the transformations applied, are outlined in the tables below. This structured approach ensures that the data is optimally organized for subsequent analysis stages.


| Script Name          | Source Database Name | Source Table Name        | Destination Database Name | Destination Table Name | Destination Table Columns                                          |
|----------------------|----------------------|--------------------------|---------------------------|-----------------------|--------------------------------------------------------------------|
| `Script1_1_Admission.py`  | x_mimiciv_hosp       | admissions               | S_Admission               | 1_Admission           | Some columns from the admission table.                             |
| `Script2_2_Admission.py`  | x_mimiciv_hosp       | admissions               | S_Admission               | 2_Admission           | Some columns from the admission table.                             |
| `Script3_1_DRG.py`        | x_mimiciv_hosp       | drgcodes                 | S_DRG                     | 1_DRG                 | Same columns                                                       |
| `Script4_1_Microbiology.py` | x_mimiciv_hosp     | microbiologyevents       | S_Microbiology            | 1_Microbiology        | Same columns                                                       |
| `Script5_1_OMR.py`        | x_mimiciv_hosp       | omr                      | S_OMR                     | 1_OMR                 | Same columns                                                       |
| `Script6_1_Patient.py`    | x_mimiciv_hosp       | patients                 | S_Patients                | 1_Patient             | Same columns                                                       |
| `Script7_1_Provider.py`   | x_mimiciv_hosp       | provider                 | S_Providers               | 1_Provider            | Same columns                                                       |
| `Script8_1_Service.py`    | x_mimiciv_hosp       | services                 | S_Services                | 1_Service             | Same columns                                                       |
| `Script9_1_Transfers.py`  | x_mimiciv_hosp       | transfers                | S_Transfers               | 1_Transfer            | Same columns                                                       |
| `Script10_1_cptEvents.py` | x_mimiciv_hosp       | d_hcpcs, hcpcsevents     | S_cptEvent                | 1_cptEvents           | Left join hcpcsevents with d_hcpcs on hcpcs=code to include the required columns. |
| `Script11_1_pharmacy.py`  | x_mimiciv_hosp       | pharmacy                 | S_pharmacy                | 1_pharmacy            | Same columns                                                       |
| `Script12_1_eMAR.py`      | x_mimiciv_hosp       | Emar, emar_detail        | S_eMAR                    | 1_eMAR                | Left join emar with emar_detail on emar_id=emar_id to include the required columns. |
| `Script13_1_prescriptions.py` | x_mimiciv_hosp   | prescriptions            | S_prescriptions           | 1_prescriptions       | Same columns                                                       |
| `Script14_1_icdCM.py`     | x_mimiciv_hosp       | d_icd_diagnoses, diagnoses_icd | S_icd_diagnoses | 1_icdCM           | Left join diagnoses_icd with d_icd_diagnoses on icd_code=icd_code to include the required columns. |
| `Script15_1_icdPCM.py`    | x_mimiciv_hosp       | d_icd_procedures, procedures_icd | S_icd_procedures | 1_icdPCM       | Left join procedures_icd with d_icd_procedures on icd_code=icd_code to include the required columns. |
| `Script16_1_labEvents.py` | x_mimiciv_hosp       | Labevents, d_labitems    | S_labEvents               | 1_labEvents           | Left join labevents with d_labitems on itemid=itemid to include the required columns. |
| `Script17_1_POE.py`       | x_mimiciv_hosp       | Poe, poe_detail          | S_poe                     | 1_POE                 | Left join poe with poe_detail on poe_id=poe_id include the required columns. |
| `Script18_1_Caregiver.py` | x_mimiciv_icu        | caregiver                | S_Caregivers              | 1_Caregiver           | Same columns                                                       |
| `Script19_1_icuStays.py`  | x_mimiciv_icu        | icustays                 | S_icuStays                | 1_icuStays            | Same columns                                                       |
| `Script20_1_datetimeEvent.py` | x_mimiciv_icu    | d_items, datetimeevents  | S_datetimeEvents          | 1_datetimeEvent       | Left join datetimeevents with d_items on itemid=itemid include the required columns. |
| `Script21_1_ingredientEvent.py` | x_mimiciv_icu  | d_items, ingredientevents | S_ingredientEvents      | 1_ingredientEvent     | Left join ingredientevents with d_items on itemid=itemid include the required columns. |
| `Script22_1_inputEvent.py` | x_mimiciv_icu       | d_items, inputevents     | S_inputEvents             | 1_inputEvent          | Left join inputevents with d_items on itemid=itemid include the required columns. |
| `Script23_1_outputEvent.py` | x_mimiciv_icu      | d_items, outputevents    | S_outputEvents            | 1_outputEvent         | Left join outputevents with d_items on itemid=itemid include the required columns. |
| `Script24_1_procedureEvent.py` | x_mimiciv_icu   | d_items, procedureevents | S_procedureEvents         | 1_procedureEvents     | Left join procedureevents with d_items on itemid=itemid include the required columns. |
| `Script25_1_chartEvent.py` | x_mimiciv_icu       | Chartevents, d_items     | S_chartEvents             | 1_chartEvent          | Left join chartevents with d_items on itemid=itemid include the required columns. |
| `Script26_Derived1.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script27_Derived2.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script28_Derived3.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script29_Derived4.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script30_Derived5.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script31_Derived6.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script32_Derived7.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script33_Derived8.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script34_Derived9.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script35_Derived10.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script36_Derived11.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script37_Derived12.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script38_Derived13.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script39_Derived14.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |
| `Script40_Derived15.py`    | x_mimiciv_derived   | Multiple tables          | S_Derived1                | Multiple tables       | Same columns                                                       |



## Stage 2: Creating ICD-SCT mapper using the OHSI

In this step, we create a mapper between ICD-10 codes and SNOMED CT codes using the OHDSI Athena vocabulary system. 


   - **Registration and Download OHDSI Vocabular**: Register for a free account on [Athena](http://athena.ohdsi.org/). Navigate to the 'Download' tab and select all vocabularies that do not require a license by checking the checkbox at the top of the list.
   - **Bundle Creation and Download**: Enter a name for your bundle, select version 5.x, and click "Download vocabularies." You will receive an email with a link to download a zip archive of the vocabulary files.
   - **Extraction Files**: Extract the archive to the `data/raw/athena` directory in your project structure.
   - **Relevant Files**: Only the `CONCEPT.csv` and `CONCEPT_RELATIONSHIP.csv` files are needed for the mapping process.
   - **Ranaming the Files**: Rename the `CONCEPT.csv` file to `Script02_CONCEPT.csv`. Similarly, rename the `CONCEPT_RELATIONSHIP.csv` file to `Script03_CONCEPT_RELATIONSHIP.csv`. Then move these renamed files into the `Step02_Stage02_OHSI_mapper` directory for organized storage and easier access during processing.
   - **Ruilding the ICD-SNOMEDCT mapper**: Execute the scripts located in the `Step02_Stage02_OHSI_mapper` directory.


## Stage 3 - Part 1: Importing SNOMED CT Data

In this step, we import the SNOMED CT terminology into our project.

   - Download the SNOMED CT RF2 database from the [official source](https://www.nlm.nih.gov/healthit/snomedct/international.html). Ensure to acquire the latest release to benefit from the most up-to-date medical terminology data.
   - From the downloaded SNOMED CT RF2 database, specifically, the `SnomedCT_InternationalRF2_PRODUCTION_20240601T120000Z` release, we will import the following tables into our project:
     - **Descriptions**: `sct2_Description_Full-en_INT_20240601.txt` - Contains the full descriptions for SNOMED CT concepts in English. Rename the file to `Script02_Description.txt` and Move the renamed file into the `Step02_Stage03_Part01_Importing_SCT` directory
     - **Relationships**: `sct2_Relationship_Full_INT_20240601.txt` - Includes comprehensive relationship data between SNOMED CT concepts. Rename the file to `Script03_Relationship.txt` and Move the renamed file into the `Step02_Stage03_Part01_Importing_SCT` directory
     - **Text Definitions**: `sct2_TextDefinition_Full-en_INT_20240601.txt` - Provides detailed definitions of SNOMED CT terms in English. Rename the file to `Script04_TextDefinition.txt` and Move the renamed file into the `Step02_Stage03_Part01_Importing_SCT` directory
   - Finally, execute the scripts located in the `Step02_Stage03_Part01_Importing_SCT` directory.

## Stage 3 - Part 2: Processing and Extracting SNOMED CT Data

In this step, we process the SNOMED CT data imported in the previous step to extract detailed information into two structured tables. To achieve:Execute the scripts located in the `Step02_Stage03_Part02_Processing_SCT` directory.

**First Table: SNOMED CT Descriptions**
This table organizes descriptive data for SNOMED CT concepts:

- **Columns**:
  - `conceptId`: SNOMED CT code.
  - `termA`: Description of SNOMED CT IDs with their semantic tag in parentheses.
  - `termA_p1`: Description of SNOMED CT IDs without their semantic tag.
  - `termA_p2`: The semantic tag of the SNOMED CT.
  - `termB`: Another description of SNOMED CT IDs, present only for some entries.
  - `ConceptType`: Categorizes SNOMED CT into three categories: 
    - `root` (only one ID, 138875005), 
    - `top-level concept` (18 SNOMED CTs), 
    - `concept` (all other IDs besides root and top-level concepts).

**Second Table: SNOMED CT Relationships**
This table details the relationships between SNOMED CT concepts:

- **Columns**:
  - `sourceId`: The ID of the first SNOMED CT concept node.
  - `destinationId`: The ID of the second SNOMED CT concept node.



# Step3: Creation of Tables Related to Events


## Stage 1: Creating Identifier-Based Time Range Tables (IDTR)

In this step, we create tables for every possible combination of MIMIC-IV identifiers, including `subject_id`, `hadm_id`, `stay_id`, and `transfer_id`. Each table captures the maximum timestamp, minimum timestamp, latest date, and earliest date for each unique instance of these combinations. Below are the table schemas showing the combinations of identifiers and the corresponding columns they capture:

| Possible IDTR Table          | Columns |
|---------------|------------------|
| S         | Subject_id,Min Timstamp,Max Timstamp,Min Date,Max Date |
| H         | Hadm_id, Min Timstamp, Max Timstamp, Min Date, Max Date |
| I         | IcuStay_ID, Min Timstamp, Max Timstamp, Min Date, Max Date, careunit |
| T         | Transfer_id, Min Timstamp, Max Timstamp, Min Date, Max Date, Eventtype, careunit |
| SH         | Subject, Hadm_id, Min Timstamp, Max Timstamp, Min Date, Max Date |
| SI         | Subject_id, IcuStay_ID, Min Timstamp, Max Timstamp, Min Date, Max Date, careunit |
| ST         | Subject_id, Transfer_id, Min Timstamp, Max Timstamp, Min Date, Max Date, Eventtype, careunit |
| HI         | Hadm_id, IcuStay_ID, Min Timstamp, Max Timstamp, Min Date, Max Date, Careunit |
| HT         | Hadm_id, Transfer_id, Min Timstamp, Max Timstamp, Min Date, Max Date, Eventtype, careunit |
| SHI         | Subject_id, Hadm_id, IcuStay_ID, Min Timstamp, Max Timstamp, Min Date, Max Date, Careunit |
| SHT         | Subject_id, Hadm_id, Transfer_id, Min Timstamp, Max Timstamp, Min Date, Max Date, Eventtype, careunit |

To create the tables, execute all scripts located in the `Step03_Stage01_IDTR_Tables` directory. The resulting tables will be stored in the dataset named "R_TimeD."

### First Application of IDTR Tables:

It is not possible to find a table in MIMIC-IV that includes all four identifiers together. However, if MIMIC-IV tables contain a timestamp or date column, IDTR tables can be used to identify the missing identifier, provided certain conditions are met. Below is a list of conditions under which the missing identifiers can be identified using IDTR tables:

**For tables lacking "IcuStay_ID" and "careunit", and needing "IcuStay_ID" and "careunit":**
- If the table has only "Subject_id":
  - Perform a left join with the SI table on "Subject_id" and a timestamp between min and max.
- If the table has only "Hadm_id":
  - Perform a left join with the HI table on "Hadm_id" and a timestamp between min and max.
- If the table has both "Subject_id" and "Hadm_id":
  - Perform a left join with the SHI table on "Subject_id" and "Hadm_id", and a timestamp between min and max.

**For tables without "Transfer_id", "Eventtype", and "careunit", needing "Transfer_id", "Eventtype", and "careunit":**
- If the table has only "Subject_id":
  - Perform a left join with the ST table on "Subject_id" and a timestamp between min and max.
- If the table has only "Hadm_id":
  - Perform a left join with the HT table on "Hadm_id" and a timestamp between min and max.
- If the table has both "Subject_id" and "Hadm_id":
  - Perform a left join with the SHT table on "Subject_id" and "Hadm_id", and a timestamp between min and max.

**For tables without "Hadm_id", and needing "Hadm_id":**
- If the table has only "Subject_id":
  - Perform a left join with the SH table on "Subject_id" and a timestamp between min and max.
- If the table has only "IcuStay_ID":
  - Perform a left join with the HI table on "IcuStay_ID" and a timestamp between min and max.
- If the table has only "Transfer_id":
  - Perform a left join with the HT table on "Transfer_id" and a timestamp between min and max.
- If the table has both "Subject_id" and "IcuStay_ID":
  - Perform a left join with the SHI table on "Subject_id" and "IcuStay_ID", and a timestamp between min and max.
- If the table has both "Subject_id" and "Transfer_id":
  - Perform a left join with the SHT table on "Subject_id" and "Transfer_id", and a timestamp between min and max.

**For tables without "Subject_id", and needing "Subject_id":**
- If the table has only "Hadm_id":
  - Perform a left join with the SH table on "Hadm_id" and a timestamp between min and max.
- If the table has only "IcuStay_ID":
  - Perform a left join with the SI table on "IcuStay_ID" and a timestamp between min and max.
- If the table has only "Transfer_id":
  - Perform a left join with the ST table on "Transfer_id" and a timestamp between min and max.
- If the table has both "Hadm_id" and "IcuStay_ID":
  - Perform a left join with the SHI table on "Hadm_id" and "IcuStay_ID", and a timestamp between min and max.
- If the table has both "Hadm_id" and "Transfer_id":
  - Perform a left join with the SHT table on "Hadm_id" and "Transfer_id", and a timestamp between min and max.

### Second Application of IDTR Tables:

Some tables in MIMIC-IV do not have a timestamp but contain a column such as `first_day_measurementOfSomething`. In this case, IDTR tables can be used to estimate the timestamp or date for these tables. To do this, we perform a left join between these tables and the relevant IDTR table using their identifiers. Then, we add 24 hours to the minimum timestamp or date from the IDTR table and insert this as a new column in the original table. This results in new tables that include an estimated timestamp or date.

### Third Application of IDTR Tables:

- If we have a table with subject_id, dateColumn, and some other columns, but it lacks the hadm_id and the time part of dateColumn, we first create a new table with only distinct pairs of subject_id and dateColumn. Then we perform a left join with the SH table, which has hadm_id and min & max timestamps, matching on subject_id and ensuring the dateColumn falls within a date range from the SH table. This step doesn't always result in a match, leaving some fields blank.
  Next, we identify how many times a distinct subject_id and dateColumn match multiple hadm_id entries. If there’s more than one match, it’s a duplicate. We then separate the dataset into two tables: one for cases where there’s a single match and another where there are multiple matches. For cases with a single match, no further action is needed. However, for cases with multiple matches, we assign a ranking number to each match, indicating the encounter number for each distinct combination of subject_id and dateColumn. We then keep only the first ranked match and discard the others. After that, we union these two tables back together.
For any records still lacking a hadm_id, we assign a default value like 0. We may have two types of records based on whether the min and max timestamps are missing or not. For records without min and max timestamps, we convert dateColumn to a timestamp. For records with min and max timestamps, we calculate a new timestamp. If adding 12 hours to the dateColumn falls within the range, we use it; otherwise, we calculate the midpoint between the min and max times.
Finally, we merge this final table back with the original table to add both the hadm_id and the complete timestamp information.

- If we have a table with hadm_id, dateColumn, and some other columns, but it lacks the subject_id and the time part of dateColumn, we first create a new table with only distinct pairs of hadm_id and dateColumn. Then we perform a left join with the SH table, which has subject_id and min & max timestamps, matching on hadm_id.
We may have two types of records based on whether the min and max timestamps are missing or not. For records without min and max timestamps, we convert dateColumn to a timestamp. For records with min and max timestamps, we calculate a new timestamp. If adding 12 hours to the dateColumn falls within the range, we use it; otherwise, we calculate the midpoint between the min and max times. Finally, we merge this final table back with the original table to add both the subject_id and the complete timestamp information.

- If we have a table with subject_id , hadm_id, dateColumn, and some other columns, but it lacks the time part of dateColumn, we first create a new table with only distinct pairs of subject_id, hadm_id and dateColumn. Then we perform a left join with the SH table, which has subject_id, hadm_id and min & max timestamps, matching on hadm_id.
We may have two types of records based on whether the min and max timestamps are missing or not. For records without min and max timestamps, we convert dateColumn to a timestamp. For records with min and max timestamps, we calculate a new timestamp. If adding 12 hours to the dateColumn falls within the range, we use it; otherwise, we calculate the midpoint between the min and max times.
Finally, we merge this final table back with the original table to add the complete timestamp information. 


## Stage 2 – Part 1: IDTR Enrichment for OMR Data
In this stage, after preprocessing the OMR table, Since it lacks the `hadm_id`  and the time part of `chartdate`  , we first create a new table with only distinct pairs of `subject_id`  and `chartdate`  . Then we perform a left join with the SH table (one of the IDTR tables) , which has `hadm_id`  and `min` and `max` timestamps, matching on `subject_id`  and ensuring the `chartdate`   falls within a date range from the SH table. This step doesn't always result in a match, leaving some fields blank. 
Next, we identify how many times a distinct `subject_id`  and `chartdate`   match multiple `hadm_id`  entries. If there’s more than one match, it’s a duplicate. We then separate the dataset into two tables: one for cases where there’s a single match and another where there are multiple matches. For cases with a single match, no further action is needed. However, for cases with multiple matches, we assign a ranking number to each match, indicating the encounter number for each distinct combination of `subject_id`  and `chartdate`  . We then keep only the first ranked match and discard the others. After that, we union these two tables back together.
For any records still lacking a `hadm_id` , we assign a default value like 0. We may have two types of records based on whether the min and max timestamps are missing or not. For records without min and max timestamps, we convert `chartdate`   to a timestamp. For records with min and max timestamps, we calculate a new timestamp. If adding 12 hours to the `chartdate`   falls within the range, we use it; otherwise, we calculate the midpoint between the min and max times.
Finally, we merge this final table back with the original OMR table to add both the `hadm_id`  and the complete timestamp information.

To replicate the same results: Execute the scripts found in the `Step03_Stage02_Part01_IDTR_Enriched_OMR` directory.

## Stage 2 – Part 2: IDTR Enrichment for First Day Lab Data
In this stage, we perform a left join of the `first_day_lab` table with the `SI` table (one of the IDTR tables). The `Timestamp` is determined by adding 24 hours to the `min Timestamp` from the `SI` table where the `subject_id` and `stay_id` match those in the `first_day_lab` table.  
To replicate the same results: Execute the scripts found in the `Step03_Stage02_Part02_IDTR_Enriched_Lab` directory.

## Stage 2 – Part 3: IDTR Enrichment for Blood Gas Data
In this stage, we perform separate left joins for the `first_day_bg` and `first_day_bg_art` tables with the `SI` table (one of the IDTR tables). Timestamps are added by appending 24 hours to the `min Timestamp` found in the `SI` table for matches on `subject_id` and `stay_id`.  
To replicate the same results: Execute the scripts found in the `Step03_Stage02_Part03_IDTR_Enriched_BG` directory.

## Stage 2 – Part 4: IDTR Enrichment for First Day GCS Data
In this stage, we perform a left join of the `first_day_gcs` table with the `SI` table (one of the IDTR tables). A timestamp is appended to the `first_day_gcs` entries by adding 24 hours to the `min Timestamp` from the `SI` table, ensuring that matches are made on the same `subject_id` and `stay_id` as in the `first_day_gcs` table.  
To replicate the same results: Execute the scripts found in the `Step03_Stage02_Part04_IDTR_Enriched_GCS` directory.

## Stage 2 – Part 5: IDTR Enrichment for Renal Replacement Therapy (RRT) Data
In this stage, the `first_day_rrt` table includes `subject_id` and `stay_id` but lacks a timestamp. We perform a left join with the `SI` table (one of the IDTR tables) and add a timestamp by appending 24 hours to the `min Timestamp` where `subject_id` and `stay_id` match those in the `first_day_rrt` table.

Additionally, the `rrt` table contains `stay_id` but lacks `subject_id`. We perform a left join with the `SI` table to add `subject_id` where the `stay_id` matches.  
To replicate the same results: Execute the scripts found in the `Step03_Stage02_Part05_IDTR_Enriched_RRT` directory.

## Stage 2 – Part 6: IDTR Enrichment for First Day Vital Sign Data
In this stage, we enhance the `first_day_vitalsign` table, which includes `subject_id` and `stay_id` but lacks timestamp information. We perform a left join with the `SI` table (one of the IDTR tables) and append a timestamp by adding 24 hours to the `min Timestamp` where the `subject_id` and `stay_id` match those in the `first_day_vitalsign` table.  
To replicate the same results: Execute the scripts found in the `Step03_Stage02_Part06_IDTR_Enriched_Vital` directory.

## Stage 2 – Part 7: IDTR Enrichment for Height Data
In this stage, we enhance the `first_day_height` table. This table includes `subject_id` and `stay_id` but lacks a timestamp. We perform a left join with the `SI` table (one of the IDTR tables) and add a timestamp by appending 24 hours to the `min Timestamp` where the `subject_id` and `stay_id` match those in the `first_day_height` table.  
To replicate the same results: Execute the scripts found in the `Step03_Stage02_Part07_IDTR_Enriched_height` directory.

## Stage 2 – Part 8: IDTR Enrichment for Weight Data
In this stage, we first enhance the `first_day_weight` table, which includes `subject_id` and `stay_id` but lacks a timestamp. We perform a left join with the `SI` table (one of the IDTR tables) and append a timestamp by adding 24 hours to the `min Timestamp` where the `subject_id` and `stay_id` match those in the `first_day_weight` table.

Next, we consider the `weight_durations` table, which contains `stay_id` but does not include `subject_id`. We perform a left join with the `SI` table to add `subject_id` where it matches the `stay_id`.  
To replicate the same results: Execute the scripts found in the `Step03_Stage02_Part08_IDTR_Enriched_weight` directory.

## Stage 2 – Part 9: IDTR Enrichment for Urine Output Data
In this stage, we first enhance the `urine_output_rate` table, which includes `subject_id` and `stay_id` but lacks a timestamp. We perform a left join with the `SI` table (one of the IDTR tables) and add a timestamp by appending 24 hours to the `min Timestamp` where the `subject_id` and `stay_id` match those in the `first_day_urine_output` table.

Next, we focus on the `urine_output` and `urine_output_rate` tables, which contain `stay_id` but do not include `subject_id`. We perform left joins with the `SI` table to add `subject_id` where it matches the `stay_id` in each table.  
To replicate the same results: Execute the scripts found in the `Step03_Stage02_Part09_IDTR_Enriched_urine` directory.

## Stage 2 – Part 10: IDTR Enrichment for SOFA-Related Data
In this stage, we enhance the `first_day_sofa` table by adding a timestamp through a left join with the `SHI` table (one of the IDTR tables), matching `subject_id`, `hadm_id`, and `stay_id`. We append a timestamp by adding 24 hours to the `min Timestamp` where the `subject_id`, `hadm_id`, and `stay_id` match those in the `first_day_urine_output` table.

Next, we enhance the `sepsis3` table by adding `hadm_id` through a left join with the `SHI` table. We also enhance the `sofa` table by adding `subject_id` through a left join with the `SI` table. Finally, we add `stay_id` to the `suspicion_of_infection` table through a left join with the `SHI` table.  
To replicate the same results: Execute the scripts found in the `Step03_Stage02_Part10_IDTR_Enriched_sofa` directory.


## Stage 3 - Part 1: Correcting and Merging NDC Codes

In this step, we focused on correcting and standardizing NDC codes. 

- Initial Tables: `1_pharmacy`, `1_eMAR`, and `1_prescriptions`.
- We began by considering the common columns in these tables: `subject_id`, `hadm_id`, `pharmacy_id`, `poe_id`, and `route`. We unioned these tables by common columns.
- We created a unified table through a series of left joins: first with `1_eMAR`, followed by `1_prescriptions`, and finally `1_pharmacy`.
 - The resultant table, named `T4`, was then segmented into five distinct tables based on the presence or absence of NDC and product codes:
    - `u21`: NDC was not null and product_code was not null.
    - `u22`: NDC was not null and product_code was null.
    - `u23`: NDC was null and product_code was not null.
    - `u24`: NDC was null and product_code was null but medication or drug descriptions were present.
    - `u25`: Both NDC and product_code were null and no medication or drug descriptions were present.

- We exported 5732 NDC codes from `u21` and `u22`.
- Using initial data crawling techniques on platforms like [NDC List](https://ndclist.com), [HIPAASpace](https://www.hipaaspace.com), and [FDA Report](https://fda.report/), we increased the count of NDC codes to 7376 by considering additional package details.
- Out of these, 6265 codes were verified through data crawling, and correct product and package codes, proprietary names, dosages, and route names were easily found.
- 1087 codes were not found on these websites, requiring manual searches using various resources including equivalent GSN NDC, proprietary searches, RxNorm, and search engines.
- 24 codes, still unverified through manual searches, were corrected using additional information from the tables like GSN NDC and proprietary names.

- The 7376 corrected NDC codes were imported back into the project (`Script27_Data_Final.csv`) and mapped to the merged table `T4`.
- The final step involved merging the corrected NDC data with `u21`, `u22`, `u23`, `u24`, and `u25` to verify the presence of product codes and other attributes.
- Resulting Table: The final merged and corrected table was named `zzzzt5`.

To reproduce the same results, execute the scripts located in the `Step03_Stage03_Part01_Cleaning_NDC` directory sequentially.


## Stage 3 - Part 2: Correcting and Merging Lab Data from Chemistry, Hematology, and Blood Gas

In this step, we integrated and processed data from the `2_Chemistry`, `2_Hematology`, and `2_Blood_Gas` tables. The goal was to consolidate these datasets, refine the fluid types, and enhance the labeling for clearer analysis.

- Data Union: We combined the `2_Chemistry`, `2_Hematology`, and `2_Blood_Gas` tables into a single dataset.
- Data Exportation: We exported the `category` (whether related to chemistry, hematology, or blood gas), `fluid`, and `label` columns into a CSV file.
- We manually modified the `fluid` descriptions to categories that were narrower and more specific than the original labels. For instance, `blood` was categorized into `Peripheral Blood Sample analysis - Whole`, `Peripheral Blood Sample analysis - Plasma`, etc. Also We enhanced the `label` with short synonyms, added units of measurement, and defined normal ranges.
- Final Output: We saved the refined data as `Script04_Data.csv` and imported it back into the project.

To reproduce the same results, execute the scripts located in the `Step03_Stage03_Part02_Cleaning_CHB` directory sequentially.

## Stage 4 - Part 1: Segmentation and Merging of the POE Table

In this stage, the `1_POE` table is divided into ten tables, each focusing on specific field types that categorize data relevant to different aspects of patient orders and events.

To achieve the same results, execute the scripts located in the `Step03_Stage04_Part01_Segment&Merge_POE` directory.

## Stage 4 - Part 2: Segmentation and Merging of the 1_OMR Table

In this stage, the `1_OMR` table is segmented into 5 tables based on the medical and treatment history of patients in the OMR table.

To achieve the same results, execute the scripts located in the `Step03_Stage04_Part02_Segment&Merge_OMR` directory.

## Stage 4 - Part 3: Segmentation and Merging of Lab Data from Chemistry, Hematology, and Blood Gas

In this stage, lab tests related to chemistry, hematology, and blood gas are segmented into 35 distinct tables based on the new fluid categories.

To achieve the same results, execute the scripts located in the `Step03_Stage04_Part03_Segment&Merge_CHB` directory.

## Stage 4 - Part 4: Segmentation and Merging of the KDIGO-Related Tables

In this stage, we combine `kdigo_creatinine`, `kdigo_stages`, and `kdigo_uo` into a single table.

To achieve the same results, execute the scripts located in the `Step03_Stage04_Part04_Segment&Merge_KDIGO` directory.

## Stage 4 - Part 5: Segmentation and Merging of the Vasopressor Data

In this stage, we focus on preprocessing data related to vasopressor (VASO) agents. We handle data from nine tables: `vasoactive_agent`, `dobutamine`, `dopamine`, `epinephrine`, `milrinone`, `norepinephrine`, `norepinephrine_equivalent_dose`, `phenylephrine`, and `vasopressin`. We combine the data from the aforementioned nine tables into a single table.

To achieve the same results, execute the scripts located in the `Step03_Stage04_Part05_Segment&Merge_VASO` directory.

## Stage 4 - Part 6: Segmentation and Merging of the Blood Gas Tables

In this stage, we union the `first_day_bg` and `first_day_bg_art` tables to form a consolidated table that includes comprehensive blood gas data from the first day of admission.

To achieve the same results, execute the scripts located in the `Step03_Stage04_Part06_Segment&Merge_BG` directory.

## Stage 4 - Part 7: Segmentation and Merging of the Renal Replacement Therapy (RRT) Data

In this step, we union the `first_day_rrt` and `rrt` tables to create a final table that captures all RRT-related data.

To achieve the same results, execute the scripts located in the `Step03_Stage04_Part07_Segment&Merge_RRT` directory.

## Stage 4 - Part 8: Segmentation and Merging of the Height Data

In this step, we union the `first_day_height` and `height` tables to form a final table that captures all relevant height data.

To achieve the same results, execute the scripts located in the `Step03_Stage04_Part08_Segment&Merge_Height` directory.

## Stage 4 - Part 9: Segmentation and Merging of the Weight Data

In this step, we union the `first_day_weight` and `weight_durations` tables to form a final table that encapsulates all relevant weight data.

To achieve the same results, execute the scripts located in the `Step03_Stage04_Part09_Segment&Merge_Weight` directory.

## Stage 4 - Part 10: Segmentation and Merging of the Urine Output Data

In this step, we combine the `first_day_urine_output`, `urine_output`, and `urine_output_rate` tables into a single table that encapsulates all relevant urine output data.

To achieve the same results, execute the scripts located in the `Step03_Stage04_Part10_Segment&Merge_Urine` directory.

## Stage 4 - Part 11: Segmentation and Merging of the SOFA-Related Data

In this step, we split the `suspicion_of_infection` table into three separate tables: `antibiotic`, `suspected_infection`, and `culture`.

To achieve the same results, execute the scripts located in the `Step03_Stage04_Part11_Segment&Merge_SOFA` directory.


## Stage 5: Activity Integration

In this step, we catalog clinical activities. We have discovered 95 distinct activities, and the results are organized into two tables for each activity instance.

**First Table: Activity Instances:** This table records each occurrence of an activity along with its specifics:
- **Columns**:
  - `subject_id`
  - `hadm_id`
  - `Timestamps`: The specific time at which the activity was recorded.
  - `Activity`: The name of the activity, such as Blood Gas Test or BP measurement.
  - `Activity_Synonym`: Contains abbreviations of activity labels, e.g., BGT for Blood Gas Test.
  - `Activity_Properties_ID_aggregation`: A unique foreign key ID for each distinct feature and its value, ensuring that identical occurrences are efficiently referenced.
    - Example:
      - po2=295 → Activity_Properties_ID=1
      - lactate=3.23 → Activity_Properties_ID=2
      - Blood pressure=137/79 → Activity_Properties_ID=3
      - po2=412 → Activity_Properties_ID=4 (same feature but different value)
  - `Activity_Value_ID`: A unique foreign key identifier for each distinct activity instance based on its combined features and values.
    - Example:
      - First event: Blood Gas Test with po2=295, lactate=3.23 → Activity_Value_ID=1
      - Second event: BP measurement with Blood pressure=137/79 → Activity_Value_ID=2

**Second Table: Activity Properties:** This table details the properties associated with each activity instance:
- **Columns**:
  - `Activity_Properties_ID`: Links back to the first table as a foreign key.
  - `Activity`
  - `Activity_Synonym`
  - `featureName`: The attribute name of the feature, such as po2 or Blood pressure.
  - `featureValue`: The numeric or descriptive value associated with the feature.

To replicable results in your project: Execute the scripts located in the `Step03_Stage05_Activity_Integration` directory.


## Stage 6 Part 1: Creating the Final Table - the Event Log

In this stage, we consolidate all `Activity Instances` tables created in stage 5 into a single event log table. We then perform preprocessing to address instances of missing `hadm_id` and ensure that the data accurately reflects patient encounters.

**1. Table Consolidation**: we Combined all activity instance tables from stage 5 into one comprehensive event log table.

**2. Preprocessing Missing `hadm_id`**:
   - we identified and evaluated instances where `hadm_id` is null to determine whether these should be corrected based on surrounding data or not.
   - **Situation Analysis**:
     - **Null `hadm_id` outside of Encounters (Should not be corrected)**: If a null `hadm_id` occurs between two different `hadm_id` values or outside the timestamps of known hospital admissions, it is considered valid and indicates activity outside of inpatient encounters.Example:
        | subject_id | hadm_id | start_Timestamp    | end_Timestamp      |
        |------------|---------|--------------------|--------------------|
        | 18415616   | 29138337| 2164-04-29T21:11:00| 2166-08-07T14:14:00|
        | 18415616   |         | 2166-09-07T14:14:00| 2166-10-07T14:14:00|
        | 18415616   | 29901658| 2150-11-24T09:45:00| 2150-12-24T17:00:00|
     - **Null `hadm_id` Within an Encounter (Should be Corrected)**: If a null `hadm_id` falls between timestamps enclosed by the same `hadm_id`, it suggests missing data within an inpatient encounter and should be replaced with the correct `hadm_id`.Example:
        | subject_id | hadm_id | start_Timestamp    | end_Timestamp      |
        |------------|---------|--------------------|--------------------|
        | 18415616   | 29138337| 2164-04-29T21:11:00| 2166-05-07T14:14:00|
        | 18415616   |         | 2166-05-07T14:14:00| 2166-06-07T14:14:00|
        | 18415616   | 29138337| 2166-06-07T14:14:00| 2166-08-07T14:14:00|

**3. Synonym Assignment**:
   - Assign synonyms to `hadm_id` such as Adm1, Adm2, Adm3, etc., to enhance readability and analysis. Assign synonyms like Out1, Out2, etc., to valid null `hadm_id`s to distinguish out-of-hospital activities. Then, we add another columns for other entities.
       - `Entity1_ID` is the ID of `subject_id`, and `Entity1_Origin` is "Patient" because `subject_id` is related to patients.
       - `Entity2_ID` is the ID of `hadm_id`, and `Entity2_Origin` is "Admission" because `hadm_id` is related to admissions. Sometimes, when there is no admission ID due to a patient without an inpatient encounter, `Entity2_ID` and `Entity2_Origin` are null.
       - `Entity3_ID` is the ID for `outpatient`, and `Entity3_Origin` is "Outpatient." This ID is newly created for each distinct outpatient encounter for a patient. Sometimes, when there is no outpatient ID for a patient without an outpatient encounter, `Entity3_ID` and `Entity3_Origin` are null.
       - `Entity4_ID` is the ID for the `admission sequence`, where the first admission is 1, the second admission is 2, and so on. This ID is common for the patient; for example, for all patients, the first admission is 1, the second is 2, and so on. `Entity4_Origin` is "Admission_Sequence." This ID and origin are created by us for better analysis. Sometimes, when there is no admission sequence ID due to a lack of an admission sequence encounter, `Entity4_ID` and `Entity4_Origin` are null.
       - `Entity5_ID` is the ID for the `outpatient sequence`, where the first outpatient visit is 1, the second outpatient visit is 2, and so on. This ID is common for the patient; for example, for all patients, the first outpatient visit is 1, the second is 2, and so on. `Entity5_Origin` is "Outpatient_Sequence." This ID and origin are created by us for better analysis. Sometimes, when there is no outpatient sequence ID due to a lack of an outpatient sequence encounter, `Entity5_ID` and `Entity5_Origin` are null.

**4. Additional Columns**:
   - Introduce necessary columns to the event log that may include details relevant to each activity instance, enhancing the table’s utility for analysis.

To replicable results in your project: Execute the scripts located in the `Step03_Stage06_Part01_C_EventLog` directory.




## Stage 6 Part 2: Creating the Final Table - the Activity Attributes

In this stage, we consolidated the `Activity Properties` table created in Stage 5 to form a comprehensive Activity Attributes table.

To replicable results in your project: Execute the scripts located in the `Step03_Stage06_Part02_E_ActivityAttributes` directory.


## Stage 6 Part 3: Creating the Final Table - the Activities Domain

In this stage, we categorized the 95 activities discovered in previous steps into 7 distinct domains manually. 

To replicable results in your project: Execute the scripts located in the `Step03_Stage06_Part03_F_ActivitiesDomain` directory.


## Stage 6 Part 4: Creating the Final Table - the Constrainted Node mapping 3

In this stage, we first compiled all activities identified in previous stages, then mapped them manually to align these elements with appropriate SNOMED CT concepts.

To replicable results in your project: Execute the scripts located in the `Step03_Stage06_Part04_L_CNM3` directory.



## Stage 6 Part 5: Creating the Final Table - the Constrainted Node mapping 4 part 1

In this stage, we created a table to map Activity to Activity_Domain.


To replicable results in your project: Execute the scripts located in the `Step03_Stage06_Part05_M_CNM4_1` directory.


## Stage 6 Part 6: Creating the Final Table - the Constrainted Node mapping 4 part 2

In this stage, we mapped manually Activity_Domain to SNOMEDCT_ID.

To replicable results in your project: Execute the scripts located in the `Step03_Stage06_Part06_M_CNM4_2` directory.


# Step 4: Creation of Tables Related to ICD Codes


## Stage 1: ICD Code Correction

The table below illustrates the existing issues with ICD codes in the MIMIC-IV dataset and the goals for correcting these issues:

| Existing ICD Codes in MIMIC-IV Issue          | The Goal |
|-----------------------------------------------|----------|
| Only one column of ICD codes consists of 25K non-comparable, untrustworthy ICD codes. | Having two columns of ICD codes: <br> One column with 1,760 high-level ICD codes <br> Another column with 19K comparable, trustworthy ICD codes. |
| Not in the correct format: <br> "2535" is incorrect for Diabetes insipidus. <br> "E232" is incorrect for Diabetes insipidus. | In the correct format: <br> "253.5" is correct for Diabetes insipidus. <br> "E23.2" is correct for Diabetes insipidus. |
| Mix of ICD-9 and ICD-10 codes, while ICD-9 is outdated: <br> "2535" (ICD-9) for Diabetes insipidus <br> "E232" (ICD-10) for Diabetes insipidus | Only ICD-10 codes (can easily be converted to ICD-11): <br> "2535" (ICD-9) -> "E23.2" (ICD-10) <br> "E232" (ICD-10) -> "E23.2" (ICD-10) |
| Mix of high-level and low-level ICD codes in one column: <br> "E11329" diabetes 2 with macular edema <br> "E113211" diabetes 2 with macular edema, right eye <br> "E113212" diabetes 2 with macular edema, left eye <br> "E113213" diabetes 2 with macular edema, bilateral <br> "E113219" diabetes 2 with macular edema, unspecified eye | Hierarchical ICD codes: <br> "E11" diabetes 2 <br> "E11.329" diabetes 2 with macular edema <br> "E11.3211" diabetes 2 with macular edema, right eye <br> "E11.3212" diabetes 2 with macular edema, left eye <br> "E11.3213" diabetes 2 with macular edema, bilateral <br> "E11.3219" diabetes 2 with macular edema, unspecified eye |

We export all ICD codes, along with their version and title, from MIMIC-IV and correct the codes using data crawling for mapping and comparing MIMIC-IV ICD codes to the [ICD10Data.com](http://www.icd10data.com) website. However, some ICD codes required a manual approach. The final corrected ICD codes are stored in the directory `Step04_Stage01_icdCodeCorrection` as `Script4_ICD_Correction.csv`. Scripts located insite that directory need to be executed to import a table called `mapper` into your project on Google BigQuery.


## Stage 2: Implementing ICD Code Correction

Utilizing the mapper table imported in previous stage, we proceed to correct the ICD codes within our project. This correction is achieved by executing the script located in `Step04_Stage02_implentationIcdCodeCorrection`. This script ensures that all ICD codes are updated to reflect accurate and standardized medical coding, aligning the dataset for better reliability and analysis. The corrected table is stored in the `icdCM3` and `icdCM4` tables of the `O_NonEvents_ICD6` database.



## Stage 3 - Part1 : Creating the Final Table - the ICD

In this stage, we create final ICD table that Contains essential ICD code information.

- **Columns**:
  - `ClinicalEntity`: A fixed value, "ICD".
  - `icd_code`: Includes `icd_10_root` code.
  - `icd_version`: Always 10, reflecting the conversion of all ICD codes to version 10 in Step 10.
  - `icd_code_title`: Description of the ICD code.

To replicable results in your project: Execute the scripts located in the `Step04_Stage03_Part01_H_ICD` directory.




## Stage 3 - Part2 : Creating the Final Table - the Constrainted Node mapping 1 

In this stage, we created the relationship table between `Disorder ID` and `ICD codes`. Since in MIMIC-IV disorders were already coded in ICD codes, we created 1759 hypothetical disorder names for each root ICD code, from `D1`, `D2` to `D1759`. The IDs range from 1 to 1759.

To replicable results in your project: Execute the scripts located in the `Step04_Stage03_Part03_K_CNM2` directory.



## Stage 3 - Part3 : Creating the Final Table - the Constrainted Node mapping 2 

In this stage, we utilized the mapper created in the input step with the OHDSI Athena vocabulary system to map ICD codes to SNOMED CT. This automated mapping covers the majority of the codes. For ICD codes that are not covered by the OHDSI Athena mapping, a manual approach is applied to ensure comprehensive coverage and accuracy.

To replicable results in your project: Execute the scripts located in the `Step04_Stage03_Part03_K_CNM2` directory.

## Stage 3 - Part4 :  Creating the Final Table - the Constrainted Node mapping 5

In this stage, we create all the necessary tables for constructing a supervised dataset that facilitates building the `constrained node mapping 5` model. The model needs to be able to map **Activity_Value_ID** (as an identifier), with **Activity_Attribute** (as feature names), and **Activity_Attribute_Value** (as feature values), to **Disorders** (as classes). We can also add timestamps to them for time-series analysis.

This step consists of four tables:

- For the **identifier**, containing all Activity_Value_ID entries.
- For **feature names and values**, consisting of Activity_Value_ID, Activity, Activity_Synonym, FeatureName, FeatureValue, Subject_ID, and Hadm_ID.
- For the **classes**, consisting of Activity_Value_ID, Disorders, Subject_ID, and Hadm_ID.
- Also, a table that converts **classes** to IDs of **classes**: containing disorders' names to disorders' IDs.

To replicate results in your project, execute the scripts located in the `Step04_Stage03_Part04_N_CNM5` directory.



# Step 5: Creation of Tables Related to Entity Activities

## Stage 1: Attributes Preparation

In this step, we created tables needed for entity attributes.

First, we created tables for morbidities:

  - **Multimorbidity**: Table with `icd_code` and `multimorbidity` columns, values range from `m1` to `m1759`.
  - **New Morbids**: Table with `icd_code` and `newMorbid` columns, values range from `n1` to `n1759`.
  - **Treated Morbids**: Table with `icd_code` and `treatedMorbid` columns, values range from `t1` to `t1759`.
  - **Untreated Morbids**: Table with `icd_code` and `untreatedMorbid` columns, values range from `u1` to `u1759`.

Then we combined all in the table `icdCM17_other_ent_final`.

Additionally, we created two other tables for age and gender attributes: `Q01_ageX3` and `Q01_PatientHadmGender`.

We also created tables for the relationship between entity attributes:
  - `icdCM19_adm_Dis_final`: Admission to disorders.
  - `icdCM19_adm_mm_final`: Admission to multimorbidity.
  - `icdCM19_mm_new_final`: Multimorbidity to New Morbids.
  - `icdCM19_mm_treat_final`: Multimorbidity to Treated Morbids.
  - `icdCM19_mm_unt_final`: Multimorbidity to Untreated Morbids.

To replicate results in your project, execute the scripts located in the `Step05_Stage01_Attribute_Preparation` directory.





## Stage 2 Part 1: Creating the Final Table - the Entities Attributes

In this step, we utilize tables created in the previous stage to create the final entities attributes table.

To replicate results in your project, execute the scripts located in the `Step05_Stage02_Part01_EntitiesAttributes` directory.

## Stage 2 Part 2: Creating the Final Table - the Entities Attribute Relationship

In this step, we utilize tables created in the previous stage to create the final entities attribute relationship table.

To replicate results in your project, execute the scripts located in the `Step05_Stage02_Part02_EntitiesAttributeRel` directory.


# Step 6: Creation of Tables Related to SNOMED CT Codes



## Stage 1: Refining SNOMED CT Data

In this stage, we refined the integration of SNOMED CT data by focusing on the descriptions and relationships established in previous stages. We considered those SNOMED CT codes that were mapped from Activities, Activities domain, and ICD to SNOMED CT codes up to the root concept.

- **Data Selection**:
    - SNOMED CT codes that were successfully mapped.
    - All intermediary SNOMED CT codes linking mapped codes to the root concept of SNOMED CT (138875005), ensuring a complete path from specific concepts to the top of the SNOMED hierarchy.
- **Adding the Level Column**:
  - This is an index we used that shows the distance of a SNOMED CT ID from the root SNOMED CT ID (138875005). Sometimes, there are different paths to navigate from a SNOMED CT ID to the root SNOMED CT ID, so it may have more than one level. This index facilitates and enhances the speed of queries.

To replicate results in your project, execute the scripts located in the `Step06_Stage01_SCT_Refinement` directory.

## Stage 2 Part 1: Creating the Final Table - The SNOMED CT Node

In this step, we utilize the SNOMED CT Node table created in Stage 1.

To replicate results in your project, execute the scripts located in the `Step06_Stage02_Part01_SCT_Node` directory.

## Stage 2 Part 2: Creating the Final Table - The SNOMED CT Node Relationships

In this step, we utilize the SNOMED CT Node Relationship table created in Stage 1.

To replicate results in your project, execute the scripts located in the `Step06_Stage02_Part02_SCT_REL` directory.

# Step 7: Creation of Cluster Reference Tables

This step includes the creation of two tables that facilitate clustering the dataset if the goal is to use specific parts of it.

**Table 1 columns:**
  - Number of disorders across all admissions.
  - Number of admissions.
  - Gender.
  - Anchor age.
  - Mortality status (whether they died or not).

**Table 2 columns:**
  - Entity1 ID (subject_ID)
  - Entity2 ID (hadm_ID)
  - ICD codes (codes, root codes, title, root title)
  - Number of disorders across all admissions.
  - Number of admissions.

To replicate results in your project, execute the scripts located in the `Step07_ClusterReferencesTables` directory.



# Step 8: Publishing the Final Dataset

In this step, we publish the final dataset. To replicate results in your project, execute the scripts located in the `Step08_Final_Dataset` directory.

For detailed information about the final dataset tables, including how to access them, the meaning of each table and column, and their use cases, please refer to the accompanying paper.


