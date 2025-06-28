-- change context
use role sysadmin;          -- use sysadmin role
use schema demo_db.public;  -- use default public schema
use warehouse compute_wh;   -- use compute warehouse

-- creating a simple task that runs every hours
create or replace task task_a
    warehouse = compute_wh
    schedule = '1 minute'
    as
    select current_database();

create or replace task task_b
    warehouse = compute_wh
    after task_a
    as
    select current_schema();

create or replace task task_c
    warehouse = compute_wh
    after task_b
    as
    select current_warehouse();

create or replace task task_d
    warehouse = compute_wh
    after task_b
    as
    select current_region();

create or replace task task_e
    warehouse = compute_wh
    after task_c,task_d
    as
    call my_storedprocedure();

-- resume all the task
use role accountadmin;
grant execute task, execute managed task on account to role sysadmin;
use role sysadmin;

alter task task_e resume;
alter task task_d resume;
alter task task_c resume;
alter task task_b resume;
alter task task_a resume;

alter task task_a suspend;
alter task task_b suspend;
alter task task_c suspend;
alter task task_d suspend;
alter task task_e suspend;
