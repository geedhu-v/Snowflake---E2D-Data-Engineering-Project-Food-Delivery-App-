use role sysadmin;          -- use sysadmin role
use schema demo_db.public;  -- use default public schema
use warehouse compute_wh;   -- use compute warehouse

-- without start/increment values
create or replace sequence my_seq_object;

-- customer table
create or replace transient table customer (
    id int default my_seq_object.nextval,
    cust_key number,
    name text,
    address text,
    nation_name text,
    phone text,
    acct_bal number,
    mkt_segment text
);

insert into customer
    (cust_key,name,address,nation_name,phone,acct_bal,mkt_segment)
    values
    (2590,'Customer#000002590','4ljncwzZkWWu','GERMANY','17-483-833-5072',10,'BUILDING'),
    (2604,'Customer#000002604','xwadGtfw2eby','GERMANY','17-102-545-8181',100,'MACHINERY'),
    (2622,'Customer#000002622','vocY5xVIZ8XWtbe3wzyAzYOolVqBQ','GERMANY','17-329-378-5573',200,'MACHINERY'),
    (2671,'Customer#000002671','r7PFEEl8sFMlFh1TNQ5','GERMANY','17-527-728-3381',300,'MACHINERY'),
    (2699,'Customer#000002699','GWZ023qBegx Z','GERMANY','17-131-640-7765',1000,'AUTOMOBILE'),
    (2722,'Customer#000002722','4fjqdFWmMQkV9YPf0JQ','GERMANY','17-452-432-9843',2000,'AUTOMOBILE'),
    (2793,'Customer#000002793','5PwXVH2cdELZil6YTqG24quyov','GERMANY','17-797-623-9769',20,'BUILDING'),
    (2815,'Customer#000002815','7MMtRqelT5CmTdcyMS9hCSJlS0hd','GERMANY','17-912-124-4764',30,'BUILDING'),
    (2821,'Customer#000002821','3WSNaGzMWQ sej PQGRPygZN4YjzWAH2c','GERMANY','17-964-888-4096',400,'MACHINERY'),
    (2832,'Customer#000002832','pbS7wyddGLiXrqyiuNvc0sF','GERMANY','17-524-362-3344',3000,'AUTOMOBILE');

select * from customer order by mkt_segment, acct_bal desc;


with cte_level_1 as 
-- add row number
(
    select 
        cust_key,
        mkt_segment,
        row_number() over (partition by mkt_segment order by acct_bal desc) as ranking,
        acct_bal 
    from 
        customer
    order by mkt_segment, ranking      
),
-- pick top 2 customers
cte_level_2 as 
(
    select mkt_segment, ranking, acct_bal
    from 
        cte_level_1
    where ranking <3
),
-- calculate avg based on top 2 entries
cte_level_3 as
(
    select mkt_segment, avg(acct_bal) as avg_acct_bal
    from cte_level_2
    group by mkt_segment
)
    select l1.mkt_segment,l1.ranking, l1.cust_key,l1.acct_bal,l3.avg_acct_bal, (l1.acct_bal - l3.avg_acct_bal) as acct_bal_gap
        from cte_level_1 l1 
        join cte_level_3 l3 on l1.mkt_segment = l3.mkt_segment
    order by l1.mkt_segment, l1.ranking;
