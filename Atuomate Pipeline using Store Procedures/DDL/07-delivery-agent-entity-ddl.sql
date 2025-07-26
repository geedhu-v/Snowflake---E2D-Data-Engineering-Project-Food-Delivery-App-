
use role sysadmin;
use database sp_pipeline_db;
use schema stage_sch;
use warehouse adhoc_wh;

create or replace table stage_sch.deliveryagent (
    deliveryagentid text comment 'Primary Key (Source System)',         -- primary key as text
    name text,           -- name as text, required field
    phone text,            -- phone as text, unique constraint indicated
    vehicletype text,             -- vehicle type as text
    locationid text,              -- foreign key reference as text (no constraint in snowflake)
    status text,                  -- status as text
    gender text,                  -- status as text
    rating text,                  -- rating as text
    createddate text,             -- created date as text
    modifieddate text,            -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the delivery stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create or replace stream stage_sch.deliveryagent_stm 
on table stage_sch.deliveryagent
append_only = true
comment = 'This is the append-only stream object on delivery agent table that only gets delta data';


CREATE OR REPLACE TABLE clean_sch.delivery_agent (
    delivery_agent_sk INT AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)', -- Primary key with auto-increment
    delivery_agent_id INT NOT NULL UNIQUE comment 'Primary Key (Source System)',               -- Delivery agent ID as integer
    name STRING NOT NULL,                -- Name as string, required field
    phone STRING NOT NULL,                 -- Phone as string, unique constraint
    vehicle_type STRING NOT NULL,                 -- Vehicle type as string
    location_id_fk INT comment 'Location FK(Source System)',                     -- Location ID as integer
    status STRING,                       -- Status as string
    gender STRING,                       -- Gender as string
    rating number(4,2),                        -- Rating as float
    created_dt TIMESTAMP_NTZ,          -- Created date as timestamp without timezone
    modified_dt TIMESTAMP_NTZ,         -- Modified date as timestamp without timezone

    -- Audit columns with appropriate data types
    _stg_file_name STRING,               -- Staging file name as string
    _stg_file_load_ts TIMESTAMP,         -- Staging file load timestamp
    _stg_file_md5 STRING,                -- Staging file MD5 hash as string
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Data copy timestamp with default value
)
comment = 'Delivery entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';


create or replace stream CLEAN_SCH.delivery_agent_stm 
on table CLEAN_SCH.delivery_agent
comment = 'This is the stream object on delivery agent table table to track insert, update, and delete changes';


CREATE OR REPLACE TABLE consumption_sch.delivery_agent_dim (
    delivery_agent_hk number primary key comment 'Delivery Agend Dim HK (EDW)',               -- Hash key for unique identification
    delivery_agent_id NUMBER not null comment 'Primary Key (Source System)',               -- Business key
    name STRING NOT NULL,                   -- Delivery agent name
    phone STRING UNIQUE,                    -- Phone number, unique
    vehicle_type STRING,                    -- Type of vehicle
    location_id_fk NUMBER NOT NULL comment 'Location FK (Source System)',                     -- Location ID
    status STRING,                          -- Current status of the delivery agent
    gender STRING,                          -- Gender
    rating NUMBER(4,2),                     -- Rating with one decimal precision
    eff_start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Effective start date
    eff_end_date TIMESTAMP,                 -- Effective end date (NULL for active record)
    is_current BOOLEAN DEFAULT TRUE
)
comment =  'Dim table for delivery agent entity with SCD2 support.';