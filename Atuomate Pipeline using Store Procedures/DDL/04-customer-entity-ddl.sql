use role sysadmin;
use database sp_pipeline_db;
use schema stage_sch;
use warehouse adhoc_wh;

-- create restaurant table under stage, with all text value + audit column for copy command
create or replace table stage_sch.customer (
    customerid text,                    -- primary key as text
    name text,                          -- name as text
    mobile text WITH TAG (common.pii_policy_tag = 'PII'),                        -- mobile number as text
    email text WITH TAG (common.pii_policy_tag = 'EMAIL'),                         -- email as text
    loginbyusing text,                  -- login method as text
    gender text WITH TAG (common.pii_policy_tag = 'PII'),                        -- gender as text
    dob text WITH TAG (common.pii_policy_tag = 'PII'),                           -- date of birth as text
    anniversary text,                   -- anniversary as text
    preferences text,                   -- preferences as text
    createddate text,                   -- created date as text
    modifieddate text,                  -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the customer stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

-- Stream object to capture the changes. 
create or replace stream stage_sch.customer_stm 
on table stage_sch.customer
append_only = true
comment = 'This is the append-only stream object on customer table that only gets delta data';


-- Part-2 Clean Layer

CREATE OR REPLACE TABLE CLEAN_SCH.CUSTOMER (
    
    CUSTOMER_SK NUMBER AUTOINCREMENT PRIMARY KEY,                -- Auto-incremented primary key
    CUSTOMER_ID STRING NOT NULL,                                 -- Customer ID
    NAME STRING(100) NOT NULL,                                   -- Customer name
    MOBILE STRING(15)  WITH TAG (common.pii_policy_tag = 'PII'),  -- Mobile number, accommodating international format
    EMAIL STRING(100) WITH TAG (common.pii_policy_tag = 'EMAIL'),  -- Email
    LOGIN_BY_USING STRING(50),                                   -- Method of login (e.g., Social, Google, etc.)
    GENDER STRING(10) WITH TAG (common.pii_policy_tag = 'PII'),   -- Gender
    DOB DATE WITH TAG (common.pii_policy_tag = 'PII'),            -- Date of birth in DATE format
    ANNIVERSARY DATE,                                            -- Anniversary in DATE format
    PREFERENCES STRING,                                          -- Customer preferences
    CREATED_DT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,           -- Record creation timestamp
    MODIFIED_DT TIMESTAMP_TZ,                                    -- Record modification timestamp, allows NULL if not modified

    -- Additional audit columns
    _STG_FILE_NAME STRING,                                       -- File name for audit
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,                             -- File load timestamp
    _STG_FILE_MD5 STRING,                                        -- MD5 hash for file content
    _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP        -- Copy data timestamp
)
comment = 'Customer entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';


-- Stream object to capture the changes. 
create or replace stream CLEAN_SCH.customer_stm 
on table CLEAN_SCH.customer
comment = 'This is the stream object on customer entity to track insert, update, and delete changes';


-- create dim table 
CREATE OR REPLACE TABLE CONSUMPTION_SCH.CUSTOMER_DIM (
    CUSTOMER_HK NUMBER PRIMARY KEY,               -- Surrogate key for the customer
    CUSTOMER_ID STRING NOT NULL,                                 -- Natural key for the customer
    NAME STRING(100) NOT NULL,                                   -- Customer name
    MOBILE STRING(15) WITH TAG (common.pii_policy_tag = 'PII'),   -- Mobile number
    EMAIL STRING(100) WITH TAG (common.pii_policy_tag = 'EMAIL'),  -- Email
    LOGIN_BY_USING STRING(50),                                   -- Method of login
    GENDER STRING(10) WITH TAG (common.pii_policy_tag = 'PII'),                                           -- Gender
    DOB DATE WITH TAG (common.pii_policy_tag = 'PII'),                                                    -- Date of birth
    ANNIVERSARY DATE,                                            -- Anniversary
    PREFERENCES STRING,                                          -- Preferences
    EFF_START_DATE TIMESTAMP_TZ,                                 -- Effective start date
    EFF_END_DATE TIMESTAMP_TZ,                                   -- Effective end date (NULL if active)
    IS_CURRENT BOOLEAN                                           -- Flag to indicate the current record
)
COMMENT = 'Customer Dimension table with SCD Type 2 handling for historical tracking.';


