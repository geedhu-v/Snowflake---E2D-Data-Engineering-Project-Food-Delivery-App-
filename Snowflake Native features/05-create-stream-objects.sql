
-- Data Engineering Simplified YouTube Channel
-- https://www.youtube.com/c/DataEngineeringSimplified

-- Snowflake Stream & Change Data Capture (Snowflake Hands-on Tutorial)https://youtu.be/DXI0GDSwE_E
-- Snowflake Unique Features (Snowflake Hands-on Tutorial) https://youtu.be/-MZrCpoAUs0
-- Snowflake Must Know New Objects (Snowflake Hands-on Tutorial)  https://youtu.be/S5NwU2o2Exg



-- change context
use role sysadmin;          -- use sysadmin role
use schema demo_db.public;  -- use default public schema
use warehouse compute_wh;   -- use compute warehouse

-- without start/increment values
create or replace sequence my_seq_object_01;

-- customer table
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

insert into customer_02
    (cust_key,name,address,nation_name,phone,acct_bal,mkt_segment)
    values
    (2590,'Customer#000002590','4ljncwzZkWWu','GERMANY','17-483-833-5072',9852.99,'BUILDING'),
    (2604,'Customer#000002604','xwadGtfw2eby','GERMANY','17-102-545-8181',3382.45,'MACHINERY'),
    (2622,'Customer#000002622','vocY5xVIZ8XW','GERMANY','17-329-378-5573',5263.92,'MACHINERY'),
    (2671,'Customer#000002671','r7PFEEl8sFMl','GERMANY','17-527-728-3381',3981.92,'MACHINERY'),
    (2699,'Customer#000002699','GWZ023qBegxZ','GERMANY','17-131-640-7765',3193.80,'AUTOMOBILE');

-- run select query on my customer table and check teh columns and column value
select *from customer_02;

-- lets create a stream object 
create or replace stream customer_stm on table customer_02;

-- select stream object & check the column values
select * from customer_stm;

-- now lets add one record to customer table
insert into customer_02
    (cust_key,name,address,nation_name,phone,acct_bal,mkt_segment)
    values
    (2832,'Customer#000002832','pbS7wyddGLiXrqyiuNvc0sF','GERMANY','17-524-362-3344',960.50,'AUTOMOBILE');


-- now lest check the stream object for insert statement in base table
select *from customer_stm;

-- now lest check the stream object for delete operation
select *from customer_stm;

-- now lets delete one record;
delete from  customer_02 where cust_key = 2604;

-- now lest check the stream object for update operation
select *from customer_stm;


-- now lets update one record;
update customer_02 set mkt_segment = 'AUTOMOBILE' where cust_key = 2590;

-- now lest check the stream object for update operation
select *from customer_stm order by id;

-- we can create multiple stream on the same table
-- lets create a stream object 
create or replace stream customer_stm_01 on table customer_02;

create or replace stream customer_stm_02 on table customer_02 append_only = True;

delete from  customer_02 where cust_key = 2699;

select * from customer_stm_01;
select * from customer_stm_02;




