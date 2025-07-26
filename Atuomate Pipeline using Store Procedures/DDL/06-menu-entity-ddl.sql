use role sysadmin;
use database sp_pipeline_db;
use schema stage_sch;
use warehouse adhoc_wh;


create or replace table stage_sch.menu (
    menuid text comment 'Primary Key (Source System)',                   -- primary key as text
    restaurantid text comment 'Restaurant FK(Source System)',             -- foreign key reference as text (no constraint in snowflake)
    itemname text,                 -- item name as text
    description text,              -- description as text
    price text,                    -- price as text (no decimal constraint)
    category text,                 -- category as text
    availability text,             -- availability as text
    itemtype text,                 -- item type as text
    createddate text,              -- created date as text
    modifieddate text,             -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the menu stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

-- Stream object to capture the changes. 
create or replace stream stage_sch.menu_stm 
on table stage_sch.menu
append_only = true
comment = 'This is the append-only stream object on menu entity that only gets delta data';

CREATE OR REPLACE TABLE clean_sch.menu (
    Menu_SK INT AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)',  -- Auto-incrementing primary key for internal tracking
    Menu_ID INT NOT NULL UNIQUE comment 'Primary Key (Source System)' ,             -- Unique and non-null Menu_ID
    Restaurant_ID_FK INT comment 'Restaurant FK(Source System)' ,                      -- Identifier for the restaurant
    Item_Name STRING not null,                        -- Name of the menu item
    Description STRING not null,                     -- Description of the menu item
    Price DECIMAL(10, 2) not null,                   -- Price as a numeric value with 2 decimal places
    Category STRING,                        -- Food category (e.g., North Indian)
    Availability BOOLEAN,                   -- Availability status (True/False)
    Item_Type STRING,                        -- Dietary classification (e.g., Vegan)
    Created_dt TIMESTAMP_NTZ,               -- Date when the record was created
    Modified_dt TIMESTAMP_NTZ,              -- Date when the record was last modified

    -- Audit columns for traceability
    _STG_FILE_NAME STRING,                  -- Source file name
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,        -- Timestamp when data was loaded from the staging layer
    _STG_FILE_MD5 STRING,                   -- MD5 hash of the source file
    _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP -- Timestamp when data was copied to the clean layer
)
comment = 'Menu entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create or replace stream CLEAN_SCH.menu_stm 
on table CLEAN_SCH.menu
comment = 'This is the stream object on menu table table to track insert, update, and delete changes';


CREATE OR REPLACE TABLE consumption_sch.menu_dim (
    Menu_Dim_HK NUMBER primary key comment 'Menu Dim HK (EDW)',                         -- Hash key generated for Menu Dim table
    Menu_ID INT NOT NULL comment 'Primary Key (Source System)',                       -- Unique and non-null Menu_ID
    Restaurant_ID_FK INT NOT NULL comment 'Restaurant FK (Source System)',                          -- Identifier for the restaurant
    Item_Name STRING,                            -- Name of the menu item
    Description STRING,                         -- Description of the menu item
    Price DECIMAL(10, 2),                       -- Price as a numeric value with 2 decimal places
    Category STRING,                            -- Food category (e.g., North Indian)
    Availability BOOLEAN,                       -- Availability status (True/False)
    Item_Type STRING,                           -- Dietary classification (e.g., Vegan)
    EFF_START_DATE TIMESTAMP_NTZ,               -- Effective start date of the record
    EFF_END_DATE TIMESTAMP_NTZ,                 -- Effective end date of the record
    IS_CURRENT BOOLEAN                         -- Flag to indicate if the record is current (True/False)
)
COMMENT = 'This table stores the dimension data for the menu items, tracking historical changes using SCD Type 2. Each menu item has an effective start and end date, with a flag indicating if it is the current record or historical. The hash key (Menu_Dim_HK) is generated based on Menu_ID and Restaurant_ID.';

