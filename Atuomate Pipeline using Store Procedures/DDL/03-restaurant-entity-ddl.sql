-- change context
use role sysadmin;
use schema sp_pipeline_db.stage_sch;
use warehouse adhoc_wh;

-- create restaurant table under stage location, with all text value + audit column for copy command
create or replace table stage_sch.restaurant (
    restaurantid text,      
    name text ,                                         -- restaurant name, required field
    cuisinetype text,                                    -- type of cuisine offered
    pricing_for_2 text,                                  -- pricing for two people as text
    restaurant_phone text WITH TAG (common.pii_policy_tag = 'SENSITIVE'),                               -- phone number as text
    operatinghours text,                                 -- restaurant operating hours
    locationid text ,                                    -- location id, default as text
    activeflag text ,                                    -- active status
    openstatus text ,                                    -- open status
    locality text,                                       -- locality as text
    restaurant_address text,                             -- address as text
    latitude text,                                       -- latitude as text for precision
    longitude text,                                      -- longitude as text for precision
    createddate text,                                    -- record creation date
    modifieddate text,                                   -- last modified date

    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the restaurant stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.'
;

-- Stream object to capture the changes. 
create or replace stream stage_sch.restaurant_stm 
on table stage_sch.restaurant
append_only = true
comment = 'This is the append-only stream object on restaurant table that only gets delta data';



-- the restaurant table where data types are defined. 
create or replace table clean_sch.restaurant (
    restaurant_sk number autoincrement primary key,              -- primary key with auto-increment
    restaurant_id number unique,                                        -- restaurant id without auto-increment
    name string(100) not null,                                   -- restaurant name, required field
    cuisine_type string,                                         -- type of cuisine offered
    pricing_for_two number(10, 2),                               -- pricing for two people, up to 10 digits with 2 decimal places
    restaurant_phone string(15) WITH TAG (common.pii_policy_tag = 'SENSITIVE'), -- phone number, supports 10-digit or international format
    operating_hours string(100),                                  -- restaurant operating hours
    location_id_fk number,                                       -- reference id for location, defaulted to 1
    active_flag string(10),                                      -- indicates if the restaurant is active
    open_status string(10),                                      -- indicates if the restaurant is currently open
    locality string(100),                                        -- locality of the restaurant
    restaurant_address string,                                   -- address of the restaurant, supports longer text
    latitude number(9, 6),                                       -- latitude with 6 decimal places for precision
    longitude number(9, 6),                                      -- longitude with 6 decimal places for precision
    created_dt timestamp_tz,                                     -- record creation date
    modified_dt timestamp_tz,                                    -- last modified date, allows null if not modified

    -- additional audit columns
    _stg_file_name string,                                       -- file name for audit
    _stg_file_load_ts timestamp_ntz,                             -- file load timestamp for audit
    _stg_file_md5 string,                                        -- md5 hash for file content for audit
    _copy_data_ts timestamp_ntz default current_timestamp        -- timestamp when data is copied, defaults to current timestamp
)
comment = 'Restaurant entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- create standard stream object on the restaurant stage table
create or replace stream clean_sch.restaurant_stm 
on table clean_sch.restaurant
comment = 'This is a standard stream object on the clean restaurant table to track insert, update, and delete changes';

-- now define dim table for restaurant.
CREATE OR REPLACE TABLE CONSUMPTION_SCH.RESTAURANT_DIM (
    RESTAURANT_HK NUMBER primary key,                   -- Hash key for the restaurant location
    RESTAURANT_ID NUMBER,                   -- Restaurant ID without auto-increment
    NAME STRING(100),                       -- Restaurant name
    CUISINE_TYPE STRING,                    -- Type of cuisine offered
    PRICING_FOR_TWO NUMBER(10, 2),          -- Pricing for two people
    RESTAURANT_PHONE STRING(15) WITH TAG (common.pii_policy_tag = 'SENSITIVE'),-- Restaurant phone number
    OPERATING_HOURS STRING(100),            -- Restaurant operating hours
    LOCATION_ID_FK NUMBER,                  -- Foreign key reference to location
    ACTIVE_FLAG STRING(10),                 -- Indicates if the restaurant is active
    OPEN_STATUS STRING(10),                 -- Indicates if the restaurant is currently open
    LOCALITY STRING(100),                   -- Locality of the restaurant
    RESTAURANT_ADDRESS STRING,              -- Full address of the restaurant
    LATITUDE NUMBER(9, 6),                  -- Latitude for the restaurant's location
    LONGITUDE NUMBER(9, 6),                 -- Longitude for the restaurant's location
    EFF_START_DATE TIMESTAMP_TZ,            -- Effective start date for the record
    EFF_END_DATE TIMESTAMP_TZ,              -- Effective end date for the record (NULL if active)
    IS_CURRENT BOOLEAN                     -- Indicates whether the record is the current version
)
COMMENT = 'Dimensional table for Restaurant entity with hash keys and SCD enabled.';