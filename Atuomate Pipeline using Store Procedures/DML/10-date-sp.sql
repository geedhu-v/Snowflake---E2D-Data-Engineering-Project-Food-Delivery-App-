use role sysadmin;
use warehouse adhoc_wh;
use database sp_pipeline_db;


CREATE OR REPLACE PROCEDURE clean_sch.date_dim_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET clean_dml_statement := 'delete from consumption_sch.date_dim';
    -- clean old data
    EXECUTE IMMEDIATE clean_dml_statement;
    
    LET insert_dml_statement := 
    '
        insert into consumption_sch.date_dim  
        with recursive my_date_dim_cte as 
        (
            -- anchor clause
            select 
                current_date() as today,
                year(today) as year,
                quarter(today) as quarter,
                month(today) as month,
                week(today) as week,
                dayofyear(today) as day_of_year,
                dayofweek(today) as day_of_week,
                day(today) as day_of_the_month,
                dayname(today) as day_name

                union all

            -- recursive clause
            select 
                dateadd(''day'', -1, today) as today_r,
                year(today_r) as year,
                quarter(today_r) as quarter,
                month(today_r) as month,
                week(today_r) as week,
                dayofyear(today_r) as day_of_year,
                dayofweek(today_r) as day_of_week,
                day(today_r) as day_of_the_month,
                dayname(today_r) as day_name
            from 
                my_date_dim_cte
            where 
                today_r > (select date(min(order_date)) from clean_sch.orders)
        )
        select 
            hash(SHA1_hex(today)) as DATE_DIM_HK,
            today ,                     -- The actual calendar date
            YEAR,                                   -- Year
            QUARTER,                                -- Quarter (1-4)
            MONTH,                                  -- Month (1-12)
            WEEK,                                   -- Week of the year
            DAY_OF_YEAR,                            -- Day of the year (1-365/366)
            DAY_OF_WEEK,                            -- Day of the week (1-7)
            DAY_OF_THE_MONTH,                       -- Day of the month (1-31)
            DAY_NAME     
        from my_date_dim_cte;
    ';

    -- run the copy command
    EXECUTE IMMEDIATE insert_dml_statement;

    -- if all goes good, the retunr will be executed
    return 'Date Dim SP Exeucted without any issue';

    -- in case the excption occurs, it will come to this point 
    EXCEPTION
        WHEN EXPRESSION_ERROR THEN
            RETURN 'Date Dim SP Failed with EXPRESSION_ERROR Exception';
        WHEN STATEMENT_ERROR THEN
            RETURN 'Date Dim SP Failed with STATEMENT_ERROR Exception';
        WHEN OTHER THEN
            RETURN 'Date Dim SP Failed with OTHER Exception';
END;

--call clean_sch.date_dim_sp();
-- --------------------------------------------------
