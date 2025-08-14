-- use sysadmin role.
use role sysadmin;
use warehouse adhoc_wh;

-- create development sandbox database/schema if it does not exist.
create database if not exists sp_pipeline_db;
use database sp_pipeline_db;

-- create all 4 schema objects.
create schema if not exists stage_sch;
create schema if not exists clean_sch;
create schema if not exists consumption_sch;
create schema if not exists common;

-- change the schema context
use schema stage_sch;

 -- create file format to process the CSV file
  create file format if not exists stage_sch.csv_file_format 
        type = 'csv' 
        compression = 'auto' 
        field_delimiter = ',' 
        record_delimiter = '\n' 
        skip_header = 1 
        field_optionally_enclosed_by = '\042' 
        null_if = ('\\N');

-- create snowflake internal stage
create stage stage_sch.csv_stg
    directory = ( enable = true )
    comment = 'this is the snowflake internal stage';

-- create tag objects
create or replace tag 
    common.pii_policy_tag 
    allowed_values 'PII','PRICE','SENSITIVE','EMAIL'
    comment = 'This is PII policy tag object';

-- create masking policies
create or replace masking policy 
    common.pii_masking_policy as (pii_text string)
    returns string -> 
    to_varchar('** PII **');

-- masking policy for email
create or replace masking policy 
    common.email_masking_policy as (email_text string)
    returns string -> 
    to_varchar('** EAMIL **');

-- masking policy for phone
create or replace masking policy 
    common.phone_masking_policy as (phone string)
    returns string -> 
    to_varchar('** Phone **');
