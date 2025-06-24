-- Data Engineering Simplified YouTube Channel
-- https://www.youtube.com/c/DataEngineeringSimplified

-- How to work with external table in Snowflake  (Snowflake Hands-on Tutorial) https://youtu.be/w9BQsOlJc5s
-- Snowflake Unique Features (Snowflake Hands-on Tutorial) https://youtu.be/-MZrCpoAUs0
-- Snowflake Must Know New Objects (Snowflake Hands-on Tutorial)  https://youtu.be/S5NwU2o2Exg

-- change context
use role sysadmin;          -- use sysadmin role
create database demo_db;    -- create demo_db database
use schema demo_db.public;  -- use default public schema
use warehouse compute_wh;   -- use compute warehouse

-- 

-- snowflake offers 4 type of stage objects
-- external stage object
-- internal stage objects
-- table stage objects
-- user stage objects

-- creating an external stage
create or replace stage my_ext_stg 
    url = 's3://snowflake-workshop-lab/weather-nyc' 
    comment = 'an external stage with aws/s3 object storage';

-- creating an internal stage
create or replace stage my_int_stg 
    comment = 'an internal stage';

-- describe the stage object
desc stage my_ext_stg;
desc stage my_int_stg;

-- list all the stage objects within database and schema
show stages;

-- list all the files stored in a stage location.
list @my_ext_stg;

list @my_int_stg;


-- lets talk about table stage
create or replace transient table demo_db.public.customer (
    cust_key number,
    name text,
    address text,
    nation_name text,
    phone text,
    acct_bal number,
    mkt_segment text
);

list @%customer;

-- list user stage
list @~;
