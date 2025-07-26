
use role sysadmin;
use database sp_pipeline_db;
use schema stage_sch;
use warehouse adhoc_wh;

create or replace table stage_sch.orders (
    orderid text comment 'Primary Key (Source System)',                  -- primary key as text
    customerid text comment 'Customer FK(Source System)',               -- foreign key reference as text (no constraint in snowflake)
    restaurantid text comment 'Restaurant FK(Source System)',             -- foreign key reference as text (no constraint in snowflake)
    orderdate text,                -- order date as text
    totalamount text,              -- total amount as text (no decimal constraint)
    status text,                   -- status as text
    paymentmethod text,            -- payment method as text
    createddate text,              -- created date as text
    modifieddate text,             -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the order stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';


create or replace stream stage_sch.orders_stm 
on table stage_sch.orders
append_only = true
comment = 'This is the append-only stream object on orders entity that only gets delta data';

CREATE OR REPLACE TABLE CLEAN_SCH.ORDERS (
    ORDER_SK NUMBER AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)',                -- Auto-incremented primary key
    ORDER_ID BIGINT UNIQUE comment 'Primary Key (Source System)',                      -- Primary key inferred as BIGINT
    CUSTOMER_ID_FK BIGINT comment 'Customer FK(Source System)',                   -- Foreign key inferred as BIGINT
    RESTAURANT_ID_FK BIGINT comment 'Restaurant FK(Source System)',                 -- Foreign key inferred as BIGINT
    ORDER_DATE TIMESTAMP,                 -- Order date inferred as TIMESTAMP
    TOTAL_AMOUNT DECIMAL(10, 2),          -- Total amount inferred as DECIMAL with two decimal places
    STATUS STRING,                        -- Status as STRING
    PAYMENT_METHOD STRING,                -- Payment method as STRING
    created_dt timestamp_tz,                                     -- record creation date
    modified_dt timestamp_tz,                                    -- last modified date, allows null if not modified

    -- additional audit columns
    _stg_file_name string,                                       -- file name for audit
    _stg_file_load_ts timestamp_ntz,                             -- file load timestamp for audit
    _stg_file_md5 string,                                        -- md5 hash for file content for audit
    _copy_data_ts timestamp_ntz default current_timestamp        -- timestamp when data is copied, defaults to current timestamp
)
comment = 'Order entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- Stream object to capture the changes. 
create or replace stream CLEAN_SCH.ORDERS_stm 
on table CLEAN_SCH.ORDERS
comment = 'This is the stream object on ORDERS table table to track insert, update, and delete changes';

