-- Data Engineering Simplified YouTube Channel
-- https://www.youtube.com/c/DataEngineeringSimplified

-- Snowflake Unique Features (Snowflake Hands-on Tutorial) https://youtu.be/-MZrCpoAUs0
-- Snowflake Must Know New Objects (Snowflake Hands-on Tutorial)  https://youtu.be/S5NwU2o2Exg

-- change context
use role sysadmin;          -- use sysadmin role
use schema demo_db.public;  -- use default public schema
use warehouse compute_wh;   -- use compute warehouse

-- without start/increment values
create or replace sequence my_seq_object_01;

-- list all sequences
show sequences like 'MY%';

-- desc sequence object
desc sequence my_seq_object_01;


-- with start value
create or replace sequence my_seq_object_02
    start = 0
    increment = 2;

-- negative increment
create or replace sequence my_seq_object_03
    start = 0
    increment = -2;

-- select from seq object    
select my_seq_object_01.nextval, my_seq_object_02.nextval,my_seq_object_03.nextval;

-- check seq objects..
show sequences like 'MY%';

create or replace transient table customer_02 (
    id int default my_seq_object_01.nextval,
    cust_key number,
    name text,
    address text,
    nation_name text,
    phone text,
    acct_bal number,
    mkt_segment text
);

desc table customer_02;
