use role sysadmin;

use database sp_pipeline_db;
use schema stage_sch;
use warehouse adhoc_wh;

-- this table may have additional information like picked time, accept time etc.
create or replace table stage_sch.delivery (
    deliveryid text comment 'Primary Key (Source System)',                           -- foreign key reference as text (no constraint in snowflake)
    orderid text comment 'Order FK (Source System)',                           -- foreign key reference as text (no constraint in snowflake)
    deliveryagentid text comment 'Delivery Agent FK(Source System)',                   -- foreign key reference as text (no constraint in snowflake)
    deliverystatus text,                    -- delivery status as text
    estimatedtime text,                     -- estimated time as text
    addressid text comment 'Customer Address FK(Source System)',                         -- foreign key reference as text (no constraint in snowflake)
    deliverydate text,                      -- delivery date as text
    createddate text,                       -- created date as text
    modifieddate text,                      -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the delivery stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

-- stream object 
create or replace stream stage_sch.delivery_stm 
on table stage_sch.delivery
append_only = true
comment = 'this is the append-only stream object on delivery table that only gets delta data';


-- create clean layer delivery 
CREATE OR REPLACE TABLE clean_sch.delivery (
    delivery_sk INT AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)', -- Primary key with auto-increment
    delivery_id INT NOT NULL comment 'Primary Key (Source System)',
    order_id_fk NUMBER NOT NULL comment 'Order FK (Source System)',                        -- Foreign key reference, converted to numeric type
    delivery_agent_id_fk NUMBER NOT NULL comment 'Delivery Agent FK (Source System)',               -- Foreign key reference, converted to numeric type
    delivery_status STRING,                 -- Delivery status, stored as a string
    estimated_time STRING,                  -- Estimated time, stored as a string
    customer_address_id_fk NUMBER NOT NULL  comment 'Customer Address FK (Source System)',                      -- Foreign key reference, converted to numeric type
    delivery_date TIMESTAMP,                -- Delivery date, converted to timestamp
    created_date TIMESTAMP,                 -- Created date, converted to timestamp
    modified_date TIMESTAMP,                -- Modified date, converted to timestamp

    -- Audit columns with appropriate data types
    _stg_file_name STRING,                  -- Source file name
    _stg_file_load_ts TIMESTAMP,            -- Source file load timestamp
    _stg_file_md5 STRING,                   -- MD5 checksum of the source file
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Metadata timestamp
)
comment = 'Delivery entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- stream on clean delivery
create or replace stream CLEAN_SCH.delivery_stm 
on table clean_sch.delivery
comment = 'This is the stream object on delivery agent table table to track insert, update, and delete changes';