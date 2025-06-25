-- change context
use role sysadmin;          -- use sysadmin role
use schema demo_db.public;  -- use default public schema
use warehouse compute_wh;   -- use compute warehouse

-- File Formats 
-- Snowflake supports 6 different type of file formats
-- csv (delimited file formats) (tsv etc)
-- JSON
-- Parquet
-- Avro
-- ORC
-- XML

-- simple csv file format
create or replace file format demo_db.public.csv_simple_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ',' 
    record_delimiter = '\n' 
    skip_header = 1;


-- with double quote  
create or replace file format demo_db.public.csv_double_q_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ',' 
    record_delimiter = '\n' 
    skip_header = 1 
    field_optionally_enclosed_by = '\042' 
    trim_space = false 
    error_on_column_count_mismatch = true;

-- with single quote
create or replace file format demo_db.public.csv_single_q_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ',' 
    record_delimiter = '\n' 
    skip_header = 1 
    field_optionally_enclosed_by = '\047' 
    trim_space = false 
    error_on_column_count_mismatch = true;

-- other types
create or replace file format demo_db.public.json_ff 
    type = 'JSON' ;

create or replace file format demo_db.public.parquet_ff 
    type = 'Parquet' ;

create or replace file format demo_db.public.avro_ff 
    type = 'AVRO' ;

create or replace file format demo_db.public.orc_ff 
    type = 'ORC' ;


-- show file formats
show file formats;

-- desc file formats
desc file format csv_single_q_ff;

-- list stage for path
list @my_int_stg;

-- query stage location using file format
select 
    t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7
    from 
@my_int_stg/customer/india/csv/ 
(file_format => 'csv_simple_ff') t;









