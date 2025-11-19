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

## Solution
To address these issues, the project implemented:  

1. **Daily Incremental Exports**  
   - Only new or updated rows are exported from the CRM, rather than contract-to-date.  

2. **Automated Loading via S3 and Snowpipe**  
   - Data is staged in an **S3 bucket**. <details> <summary><strong>Click to view SQL Script</strong></summary>
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

create or replace file format csv_fileformat
type = csv
field_delimiter =  ','
skip_header = 1
null_if = ('NULL','null')
empty_field_as_null = true;
```

</details> 
   - **Snowpipe** automatically loads staged data into Snowflake.  
   - Merge/load logic ensures data is appended or updated in the existing table.

3. **Cost Optimization**  
   - **S3 Lifecycle**: After 30 days, older files are moved to deep glacial storage to reduce cost.  
   - **Views** in Snowflake are used to avoid unnecessary storage costs, as Snowflake pricing splits compute and storage.  
   - **Stage cleanup**: Data is deleted from the Snowflake stage after 4 days, using S3 as the cheaper long-term storage.  

4. **Error Handling**  
   - An **exceptions table** captures failed or problematic rows for review and correction.  

5. **Integration Setup**  
   - Demonstrates connecting **AWS to Snowflake** using **IAM roles, integration objects, stages, and Snowpipe**.  
   - Ensures secure, automated data pipeline management.

## Impact
- Reduced load failures and improved reliability of daily data updates  
- Reduced Snowflake storage costs and optimized compute usage  
- Automated daily incremental uploads, saving processing time and operational effort  
- Clear visibility into exceptions and error handling  
