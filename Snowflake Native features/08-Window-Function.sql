use role sysadmin;          -- use sysadmin role
use schema demo_db.public;  -- use default public schema
use warehouse compute_wh;   -- use compute warehouse

-- lets operate on customer table

-- lets see the data in customer table
select * from customer;

-- 
select 
    row_number() over (order by id) as row_number,
    *
    from  customer;

select 
    rank() over (partition by mkt_segment order by acct_bal desc) as rank_number,
    *
    from  customer;

select 
    dense_rank() over (partition by mkt_segment order by acct_bal desc) as rank_number,
    *
    from  customer;use role sysadmin;          -- use sysadmin role
use schema demo_db.public;  -- use default public schema
use warehouse compute_wh;   -- use compute warehouse

-- lets operate on customer table

-- lets see the data in customer table
select * from customer;

-- 
select 
    row_number() over (order by acct_bal desc) as row_number,
    *
    from  customer;

select 
    rank() over (partition by mkt_segment order by acct_bal desc) as rank_number,
    *
    from  customer;

select 
    dense_rank() over (partition by mkt_segment order by acct_bal desc) as rank_number,
    *
    from  customer;
