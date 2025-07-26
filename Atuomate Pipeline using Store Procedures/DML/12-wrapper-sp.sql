use role sysadmin;
use warehouse adhoc_wh;
use database sp_pipeline_db;

CREATE OR REPLACE PROCEDURE common.copy_pipeline_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- run the copy command
    EXECUTE IMMEDIATE 'call stage_sch.copy_csv2stg_location_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.copy_csv2stg_restaurant_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.copy_csv2stg_customer_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.copy_csv2stg_customer_address_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.copy_csv2stg_menu_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.copy_csv2stg_delivery_agent_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.copy_csv2stg_delivery_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.copy_csv2stg_orders_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.copy_csv2stg_order_items_sp()';


    -- if all goes good, the retunr will be executed
    return 'Success';

    -- in case the excption occurs, it will come to this point 
    EXCEPTION
        WHEN EXPRESSION_ERROR THEN
            RETURN 'Main-Copy SP Failed with EXPRESSION_ERROR Exception';
        WHEN STATEMENT_ERROR THEN
            RETURN 'Main-Copy SP Failed with STATEMENT_ERROR Exception';
        WHEN OTHER THEN
            RETURN 'Main-Copy SP Failed with OTHER Exception';
END;

ALTER SESSION SET QUERY_TAG = 'Main Copy Stored Procedure';
call common.copy_pipeline_sp();

-- Stage Table To Clean Table Merge SPs
CREATE OR REPLACE PROCEDURE common.s2c_pipeline_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- run the copy command
    EXECUTE IMMEDIATE 'call stage_sch.s2c_location_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.s2c_restaurant_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.s2c_customer_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.s2c_customer_address_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.s2c_menu_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.s2c_delivery_agent_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.s2c_delivery_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.s2c_orders_sp()';
    EXECUTE IMMEDIATE 'call stage_sch.s2c_order_items_sp()';


    -- if all goes good, the retunr will be executed
    return 'Success';

    -- in case the excption occurs, it will come to this point 
    EXCEPTION
        WHEN EXPRESSION_ERROR THEN
            RETURN 'Main-S2C SP Failed with EXPRESSION_ERROR Exception';
        WHEN STATEMENT_ERROR THEN
            RETURN 'Main-S2C SP Failed with STATEMENT_ERROR Exception';
        WHEN OTHER THEN
            RETURN 'Main-S2C SP Failed with OTHER Exception';
END;

ALTER SESSION SET QUERY_TAG = 'Main S2C Stored Procedure';
call common.s2c_pipeline_sp();

CREATE OR REPLACE PROCEDURE common.c2c_pipeline_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- run the copy command
    EXECUTE IMMEDIATE 'call clean_sch.c2c_location_sp()';
    EXECUTE IMMEDIATE 'call clean_sch.c2c_restaurant_sp()';
    EXECUTE IMMEDIATE 'call clean_sch.c2c_customer_sp()';
    EXECUTE IMMEDIATE 'call clean_sch.c2c_customer_address_sp()';
    EXECUTE IMMEDIATE 'call clean_sch.c2c_menu_sp()';
    EXECUTE IMMEDIATE 'call clean_sch.c2c_delivery_agent_sp()';
    EXECUTE IMMEDIATE 'call clean_sch.date_dim_sp()';

    -- if all goes good, the retunr will be executed
    return 'Success';

    -- in case the excption occurs, it will come to this point 
    EXCEPTION
        WHEN EXPRESSION_ERROR THEN
            RETURN 'Main-C2C SP Failed with EXPRESSION_ERROR Exception';
        WHEN STATEMENT_ERROR THEN
            RETURN 'Main-C2C SP Failed with STATEMENT_ERROR Exception';
        WHEN OTHER THEN
            RETURN 'Main-C2C SP Failed with OTHER Exception';
END;

ALTER SESSION SET QUERY_TAG = 'Main C2C Stored Procedure V3';
call common.c2c_pipeline_sp();
call clean_sch.fact_table_sp();

-- ---------------------------------------

CREATE OR REPLACE PROCEDURE common.main_pipeline_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    let copy_rs RESULTSET;
    let copy_return_msg text;

    let s2c_rs RESULTSET;
    let s2c_return_msg text;

    let c2c_rs RESULTSET;
    let c2c_return_msg text;

    -- run the copy command
    -- 1st execution
    copy_rs := (EXECUTE IMMEDIATE 'call common.copy_pipeline_sp()');
    -- 1st for loop
    FOR copy_record IN copy_rs DO
        copy_return_msg := copy_record.COPY_PIPELINE_SP;
        --1st check
        IF (copy_return_msg = 'Success') THEN
            execute immediate 'select current_database()';
            --2nd execution
            s2c_rs := (EXECUTE IMMEDIATE 'call common.s2c_pipeline_sp()');
            --2nd loop
            FOR s2c_record IN s2c_rs DO
                s2c_return_msg := s2c_record.S2C_PIPELINE_SP;
                --2nd check
                IF (s2c_return_msg = 'Success') THEN
                    execute immediate 'select current_schema()';

                    --3rd execution
                    c2c_rs := (EXECUTE IMMEDIATE 'call common.c2c_pipeline_sp()');
                    --3rd loop
                    FOR c2c_record IN c2c_rs DO
                        c2c_return_msg := c2c_record.C2C_PIPELINE_SP;
                        IF (c2c_return_msg = 'Success') THEN
                            execute immediate 'select current_role()';
                            EXECUTE IMMEDIATE 'call clean_sch.fact_table_sp()';
                        END IF;
                    END FOR;

                END IF;
            END FOR;
        END IF;
    END FOR;
    -- COPY_CSV2STG_LOCATION_SP
    --EXECUTE IMMEDIATE 'call common.s2c_pipeline_sp()';
    -- EXECUTE IMMEDIATE 'call common.c2c_pipeline_sp()';

    -- if all goes good, the retunr will be executed
    return 'Main Pipeline SP Exeucted without any issue';

    -- in case the excption occurs, it will come to this point 
    EXCEPTION
        WHEN EXPRESSION_ERROR THEN
            RETURN 'Main Pipeline SP Failed with EXPRESSION_ERROR Exception';
        WHEN STATEMENT_ERROR THEN
            RETURN 'Main Pipeline SP Failed with STATEMENT_ERROR Exception';
        WHEN OTHER THEN
            RETURN 'Main Pipeline SP Failed with OTHER Exception';
END;

/*

put file:///tmp/small_files/01-location/location-initial-load.csv @sp_pipeline_db.stage_sch.csv_stg/location;
put file:///tmp/small_files/02-restaurant/restaurant-delhi+NCR.csv @sp_pipeline_db.stage_sch.csv_stg/restaurant;
put file:///tmp/small_files/03-customer/customers-initial.csv @sp_pipeline_db.stage_sch.csv_stg/customer;
put file:///tmp/small_files/04-customer-address/customer_address_initial.csv @sp_pipeline_db.stage_sch.csv_stg/customer-address;
put file:///tmp/small_files/05-menu/menu-initial-load.csv @sp_pipeline_db.stage_sch.csv_stg/menu;
put file:///tmp/small_files/06-delivery-agent/delivery-agent-initial.csv  @sp_pipeline_db.stage_sch.csv_stg/delivery-agent;
put file:///tmp/small_files/07-order/orders-initial.csv @sp_pipeline_db.stage_sch.csv_stg/orders;
put file:///tmp/small_files/08-order-item/order-Item-initial.csv @sp_pipeline_db.stage_sch.csv_stg/order-items;
put file:///tmp/small_files/09-delivery/delivery-initial-load.csv @sp_pipeline_db.stage_sch.csv_stg/delivery;

 */
ALTER SESSION SET QUERY_TAG = 'V004';
call common.main_pipeline_sp();
ALTER SESSION UNSET QUERY_TAG;

use role accountadmin;
grant execute task, execute managed task on account to role sysadmin;

use role sysadmin;

create or replace task common.main_pipeline_task
    warehouse = adhoc_wh
    schedule = '5 minute'
    as
    call common.main_pipeline_sp();

alter task common.main_pipeline_task suspend;

