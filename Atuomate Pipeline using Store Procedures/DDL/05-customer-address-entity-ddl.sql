use role sysadmin;
use database sp_pipeline_db;
use schema stage_sch;
use warehouse adhoc_wh;

create or replace table stage_sch.customeraddress (
    addressid text,                    -- primary key as text
    customerid text comment 'Customer FK (Source Data)',  -- foreign key reference as text (no constraint in snowflake)
    flatno text,                       -- flat number as text
    houseno text,                      -- house number as text
    floor text,                        -- floor as text
    building text,                     -- building name as text
    landmark text,                     -- landmark as text
    locality text,                     -- locality as text
    city text,                          -- city as text
    state text,                         -- state as text
    pincode text,                       -- pincode as text
    coordinates text,                  -- coordinates as text
    primaryflag text,                  -- primary flag as text
    addresstype text,                  -- address type as text
    createddate text,                  -- created date as text
    modifieddate text,                 -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the customer address stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create or replace stream stage_sch.customeraddress_stm 
on table stage_sch.customeraddress
append_only = true
comment = 'This is the append-only stream object on customer address table that only gets delta data';


-- 2nd layer
CREATE OR REPLACE TABLE CLEAN_SCH.CUSTOMER_ADDRESS (
    CUSTOMER_ADDRESS_SK NUMBER AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EWH)',                -- Auto-incremented primary key
    ADDRESS_ID INT comment 'Primary Key (Source Data)',                 -- Primary key as string
    CUSTOMER_ID_FK INT comment 'Customer FK (Source Data)',                -- Foreign key reference as string (no constraint in Snowflake)
    FLAT_NO STRING,                    -- Flat number as string
    HOUSE_NO STRING,                   -- House number as string
    FLOOR STRING,                      -- Floor as string
    BUILDING STRING,                   -- Building name as string
    LANDMARK STRING,                   -- Landmark as string
    locality STRING,                   -- locality as string
    CITY STRING,                       -- City as string
    STATE STRING,                      -- State as string
    PINCODE STRING,                    -- Pincode as string
    COORDINATES STRING,                -- Coordinates as string
    PRIMARY_FLAG STRING,               -- Primary flag as string
    ADDRESS_TYPE STRING,               -- Address type as string
    CREATED_DATE TIMESTAMP_TZ,         -- Created date as timestamp with time zone
    MODIFIED_DATE TIMESTAMP_TZ,        -- Modified date as timestamp with time zone

    -- Audit columns with appropriate data types
    _STG_FILE_NAME STRING,
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5 STRING,
    _COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
comment = 'Customer address entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';


-- Stream object to capture the changes. 
create or replace stream CLEAN_SCH.CUSTOMER_ADDRESS_STM
on table CLEAN_SCH.CUSTOMER_ADDRESS
comment = 'This is the stream object on customer address entity to track insert, update, and delete changes';



CREATE OR REPLACE TABLE CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM (
    CUSTOMER_ADDRESS_HK NUMBER PRIMARY KEY comment 'Customer Address HK (EDW)',        -- Surrogate key (hash key)
    ADDRESS_ID INT comment 'Primary Key (Source System)',                                -- Original primary key
    CUSTOMER_ID_FK STRING comment 'Customer FK (Source System)',                            -- Surrogate key from Customer Dimension (Foreign Key)
    FLAT_NO STRING,                                -- Flat number
    HOUSE_NO STRING,                               -- House number
    FLOOR STRING,                                  -- Floor
    BUILDING STRING,                               -- Building name
    LANDMARK STRING,                               -- Landmark
    LOCALITY STRING,                               -- Locality
    CITY STRING,                                   -- City
    STATE STRING,                                  -- State
    PINCODE STRING,                                -- Pincode
    COORDINATES STRING,                            -- Geo-coordinates
    PRIMARY_FLAG STRING,                           -- Whether it's the primary address
    ADDRESS_TYPE STRING,                           -- Type of address (e.g., Home, Office)

    -- SCD2 Columns
    EFF_START_DATE TIMESTAMP_TZ,                                 -- Effective start date
    EFF_END_DATE TIMESTAMP_TZ,                                   -- Effective end date (NULL if active)
    IS_CURRENT BOOLEAN                                           -- Flag to indicate the current record
);
