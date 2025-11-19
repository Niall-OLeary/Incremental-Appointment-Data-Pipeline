# Incremental Appointment Data Pipeline

## Description
This project improves the data ingestion process by optimizing the pipeline that loads data into Snowflake.  
It focuses on automating daily incremental uploads, reducing failures, and cutting costs while maintaining data accessibility.

## Current Business Problem
The existing infrastructure pushed **contract-to-date data through the pipeline every day**.  
As the dataset grew, the uploads increasingly failed, causing delays and inconsistencies in data availability.

Challenges included:  
- Full refresh every morning was time-consuming and resource-intensive  
- Larger uploads often failed  
- Snowflake costs (both storage and compute) increased due to frequent full uploads
- Current infrastructure did not handle exceptions (e.g. Pending appointments in the past)

## Solution
To address these issues, the project implemented:  

1. **Daily Incremental Exports**  
   - Only new or updated rows are exported from the CRM, rather than contract-to-date.  

2. **Automated Loading via S3 and Snowpipe**  
   - Data is staged in an **S3 bucket**.  
   - **Snowpipe** automatically loads staged data into Snowflake.  
   - Merge/load logic ensures data is appended or updated in the existing table.
<details> 
<summary><strong>Snowpipe Script</strong></summary>
   
```sql
-- creating snowpipe. This automatically looks into the s3 bucket and whenever a new file is added,
-- it copies into the stg_daily_appts table.
create or replace pipe daily_incremental_pipe
auto_ingest = TRUE
AS
COPY INTO STG_DAILY_APPOINTMENTS 
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, $17, $18, $19,
         METADATA$START_SCAN_TIME AS LOAD_TS, METADATA$FILENAME
    FROM @stg_daily_appointments
);

-- desc used to get notification channel arn to add to properties of s3 bucket.
desc pipe daily_incremental_pipe;
```

</details>  
<details> 
<summary><strong>Insert/Merge Script</strong></summary>
   
```sql
-- Inserts or updates the main presentation table with the most recent file from the stage
MERGE INTO DATA_WAREHOUSE.PRESENTATION.APPOINTMENTS AS t
USING (
    SELECT * 
    FROM VW_STG_DAILY_APPOINTMENTS 
    WHERE LOAD_TS = (select MAX(LOAD_TS) from VW_STG_DAILY_APPOINTMENTS)
)  AS s 
ON t.appointment_id = s.appointment_id and t.PARTICIPANT_ID = s.PARTICIPANT_ID 
WHEN MATCHED
THEN UPDATE SET
    t.PARTICIPANT_ID = s.PARTICIPANT_ID,
    t.APPOINTMENT_TITLE = s.APPOINTMENT_TITLE,
    t.APPOINTMENT_CLASS = s.APPOINTMENT_CLASS,
    t.APPOINTMENT_TYPE = s.APPOINTMENT_TYPE,
    t.ROLE_IN_APPOINTMENT = s.ROLE_IN_APPOINTMENT,
    t.APPOINTMENT_START_DATE = s.APPOINTMENT_START_DATE,
    t.APPOINTMENT_START_TIME = s.APPOINTMENT_START_TIME,
    t.APPOINTMENT_END_DATE = s.APPOINTMENT_END_DATE,
    t.APPOINTMENT_COMPLETED_DATE = s.APPOINTMENT_COMPLETED_DATE,
    t.ORGANISER_ID = s.ORGANISER_ID,
    t.APPOINTMENT_STATUS = s.APPOINTMENT_STATUS,
    t.APPOINTMENT_MAIN_STATUS = s.APPOINTMENT_MAIN_STATUS,
    t.APPOINTMENT_WORKFLOW = s.APPOINTMENT_WORKFLOW,
    t.HEALTH_REVIEW_INTERVENTION_TYPE = s.HEALTH_REVIEW_INTERVENTION_TYPE,
    t.ID_CHECK = s.ID_CHECK,
    t.DATE_CREATED = s.LOAD_TS
WHEN NOT MATCHED THEN
INSERT
    (
    PARTICIPANT_ID,
    APPOINTMENT_ID,
    APPOINTMENT_TITLE,
    APPOINTMENT_CLASS,
    APPOINTMENT_TYPE,
    ROLE_IN_APPOINTMENT,
    APPOINTMENT_START_DATE,
    APPOINTMENT_START_TIME,
    APPOINTMENT_END_DATE,
    APPOINTMENT_COMPLETED_DATE,
    ORGANISER_ID,
    APPOINTMENT_STATUS,
    APPOINTMENT_MAIN_STATUS,
    APPOINTMENT_WORKFLOW,
    HEALTH_REVIEW_INTERVENTION_TYPE,
    ID_CHECK,
    DATE_CREATED
)
VALUES (
    s.PARTICIPANT_ID,
    s.APPOINTMENT_ID,
    s.APPOINTMENT_TITLE,
    s.APPOINTMENT_CLASS,
    s.APPOINTMENT_TYPE,
    s.ROLE_IN_APPOINTMENT,
    s.APPOINTMENT_START_DATE,
    s.APPOINTMENT_START_TIME,
    s.APPOINTMENT_END_DATE,
    s.APPOINTMENT_COMPLETED_DATE,
    s.ORGANISER_ID,
    s.APPOINTMENT_STATUS,
    s.APPOINTMENT_MAIN_STATUS,
    s.APPOINTMENT_WORKFLOW,
    s.HEALTH_REVIEW_INTERVENTION_TYPE,
    s.ID_CHECK,
    s.LOAD_TS
);
```

</details> 

3. **Cost Optimization**  
   - **S3 Lifecycle**: After 30 days, older files are moved to deep glacial storage to reduce cost.  
   - **Views** in Snowflake are used to avoid unnecessary storage costs, as Snowflake pricing splits compute and storage.  
   - **Stage cleanup**: Data is deleted from the Snowflake stage after 4 days, using S3 as the cheaper long-term storage.  
<details> 
<summary><strong>Snowflake View Script</strong></summary>
   
```sql
-- This view does some basic formatting e.g. var char to date/time. Creating a table and storing this data,
-- which only differs from the staging table by formats, increases cost.
-- Therefore, creating a view which is only called when updating the main appointments table decreases storage costs.
create or replace view VW_STG_DAILY_APPOINTMENTS(
PARTICIPANT_ID,
APPOINTMENT_ID,
APPOINTMENT_TITLE,
APPOINTMENT_CLASS,
APPOINTMENT_TYPE,
ROLE_IN_APPOINTMENT,
APPOINTMENT_START_DATE,
APPOINTMENT_START_TIME,
APPOINTMENT_END_DATE,
APPOINTMENT_END_TIME,
APPOINTMENT_COMPLETED_DATE,
APPOINTMENT_COMPLETED_TIME,
ORGANISER_ID,
APPOINTMENT_STATUS,
APPOINTMENT_MAIN_STATUS,
APPOINTMENT_WORKFLOW,
HEALTH_REVIEW_INTERVENTION_TYPE,
HEALTH_REVIEW_INTERVENTION_CODE,
ID_CHECK,
LOAD_TS,
FILE_NAME
) AS
select
PARTICIPANT_ID,
APPOINTMENT_ID,
APPOINTMENT_TITLE,
APPOINTMENT_CLASS,
APPOINTMENT_TYPE,
ROLE_IN_APPOINTMENT,
COALESCE(
        TRY_TO_DATE(APPOINTMENT_START_DATE, 'DD/MM/YYYY HH24:MI:SS'),
        TRY_TO_DATE(APPOINTMENT_START_DATE, 'DD/MM/YYYY')
    ) AS APPOINTMENT_START_DATE,
APPOINTMENT_START_TIME,
COALESCE(
        TRY_TO_DATE(APPOINTMENT_END_DATE, 'DD/MM/YYYY HH24:MI:SS'),
        TRY_TO_DATE(APPOINTMENT_END_DATE, 'DD/MM/YYYY')
    ) AS APPOINTMENT_END_DATE,
APPOINTMENT_END_TIME,
    COALESCE(
        TRY_TO_DATE(APPOINTMENT_COMPLETED_DATE, 'DD/MM/YYYY HH24:MI:SS'),
        TRY_TO_DATE(APPOINTMENT_COMPLETED_DATE, 'DD/MM/YYYY')
    ) AS APPOINTMENT_COMPLETED_DATE,
APPOINTMENT_COMPLETED_TIME,
ORGANISER_ID,
APPOINTMENT_STATUS,
APPOINTMENT_MAIN_STATUS,
APPOINTMENT_WORKFLOW,
HEALTH_REVIEW_INTERVENTION_TYPE,
HEALTH_REVIEW_INTERVENTION_CODE,
ID_CHECK,
LOAD_TS,
FILE_NAME
from STG_DAILY_APPOINTMENTS
;
```

</details> 

4. **Error Handling**  
   - An **exceptions table** captures problematic rows for review and correction.
<details> 
<summary><strong>Exceptions Table Script</strong></summary>
   
```sql
-- Exceptions table
create or replace table APPOINTMENTS_EXCEPTIONS (
SELECT * FROM VW_STG_DAILY_APPOINTMENTS
where 
(appointment_main_status = 'Pending' and appointment_start_date < current_date()) or
(appointment_start_date > dateadd(month,9,current_date))
);
```

</details>   

5. **Integration Setup**  
   - Demonstrates connecting **AWS to Snowflake** using **IAM roles, integration objects, stages, and Snowpipe**.  
   - Ensures secure, automated data pipeline management.
<details> 
<summary><strong>S3 Integration SQL Script</strong></summary>
   
```sql
-- creates storage integration. Connects to the s3 bucket and provides IAM role permissions for any future stage connection.
create or replace storage integration s3_int
type = external_stage
storage_provider = S3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::997948075400:role/akg-snowflake-access-role'
storage_allowed_locations = ('s3://akg-s3-appointments-s3/base-appointments/','s3://akg-s3-appointments-s3/daily-incremental/')
STORAGE_AWS_EXTERNAL_ID = '0960';

-- description used to get IAM_USER_ARN code to add to IAM permissions in AWS.
desc integration s3_int;

-- file format used to read csv files
create or replace file format csv_fileformat
type = csv
field_delimiter =  ','
skip_header = 1
null_if = ('NULL','null')
empty_field_as_null = true;
```

</details>  

## Impact
- Reduced load failures and improved reliability of daily data updates  
- Reduced Snowflake storage costs and optimized compute usage  
- Automated daily incremental uploads, saving processing time and operational effort  
- Clear visibility into exceptions and error handling  
