use role sysadmin;
use warehouse adhoc_wh;
use database sp_pipeline_db;


CREATE OR REPLACE PROCEDURE stage_sch.copy_csv2stg_orders_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET copy_dml_statement := 
    '
        copy into stage_sch.orders (orderid, customerid, restaurantid, orderdate, totalamount, 
                  status, paymentmethod, createddate, modifieddate,
                  _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
        from (
            select 
                t.$1::number as orderid,
                t.$2::text as customerid,
                t.$3::text as restaurantid,
                t.$4::text as orderdate,
                t.$5::text as totalamount,
                t.$6::text as status,
                t.$7::text as paymentmethod,
                t.$8::text as createddate,
                t.$9::text as modifieddate,
                metadata$filename as _stg_file_name,
                metadata$file_last_modified as _stg_file_load_ts,
                metadata$file_content_key as _stg_file_md5,
                current_timestamp as _copy_data_ts
            from @stage_sch.csv_stg/orders/ t
        )
        file_format = (format_name = ''stage_sch.csv_file_format'')
        on_error = continue
    ';

    -- run the copy command
    EXECUTE IMMEDIATE copy_dml_statement;

    -- if all goes good, the retunr will be executed
    return 'csv2s SP Exeucted without any issue';

    -- in case the excption occurs, it will come to this point 
    EXCEPTION
        WHEN EXPRESSION_ERROR THEN
            RETURN 'csv2s SP Failed with EXPRESSION_ERROR Exception';
        WHEN STATEMENT_ERROR THEN
            RETURN 'csv2s SP Failed with STATEMENT_ERROR Exception';
        WHEN OTHER THEN
            RETURN 'csv2s SP Failed with OTHER Exception';
END;

--call stage_sch.copy_csv2stg_orders_sp();

-- --------------------------------------------------

-- stored procedure to run merge statement from 
-- s2c = stage to clean layer data movement
CREATE OR REPLACE PROCEDURE stage_sch.s2c_orders_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO CLEAN_SCH.ORDERS AS target
        USING STAGE_SCH.ORDERS_STM AS source
            ON target.ORDER_ID = TRY_TO_NUMBER(source.ORDERID) -- Match based on ORDER_ID
        WHEN MATCHED THEN
            -- Update existing records
            UPDATE SET
                TOTAL_AMOUNT = TRY_TO_DECIMAL(source.TOTALAMOUNT),
                STATUS = source.STATUS,
                PAYMENT_METHOD = source.PAYMENTMETHOD,
                MODIFIED_DT = TRY_TO_TIMESTAMP_TZ(source.MODIFIEDDATE),
                _STG_FILE_NAME = source._STG_FILE_NAME,
                _STG_FILE_LOAD_TS = source._STG_FILE_LOAD_TS,
                _STG_FILE_MD5 = source._STG_FILE_MD5,
                _COPY_DATA_TS = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            -- Insert new records
            INSERT (
                ORDER_ID,
                CUSTOMER_ID_FK,
                RESTAURANT_ID_FK,
                ORDER_DATE,
                TOTAL_AMOUNT,
                STATUS,
                PAYMENT_METHOD,
                CREATED_DT,
                MODIFIED_DT,
                _STG_FILE_NAME,
                _STG_FILE_LOAD_TS,
                _STG_FILE_MD5,
                _COPY_DATA_TS
            )
            VALUES (
                TRY_TO_NUMBER(source.ORDERID),
                TRY_TO_NUMBER(source.CUSTOMERID),
                TRY_TO_NUMBER(source.RESTAURANTID),
                TRY_TO_TIMESTAMP(source.ORDERDATE),
                TRY_TO_DECIMAL(source.TOTALAMOUNT),
                source.STATUS,
                source.PAYMENTMETHOD,
                TRY_TO_TIMESTAMP_TZ(source.CREATEDDATE),
                TRY_TO_TIMESTAMP_TZ(source.MODIFIEDDATE),
                source._STG_FILE_NAME,
                source._STG_FILE_LOAD_TS,
                source._STG_FILE_MD5,
                CURRENT_TIMESTAMP
            )
    ';

    -- run the copy command
    EXECUTE IMMEDIATE merge_dml_statement;

    -- if all goes good, the retunr will be executed
    return 'S2C Merge SP Exeucted without any issue';

    -- in case the excption occurs, it will come to this point 
    EXCEPTION
        WHEN EXPRESSION_ERROR THEN
            RETURN 'S2C Merge SP Failed with EXPRESSION_ERROR Exception';
        WHEN STATEMENT_ERROR THEN
            RETURN 'S2C Merge SP Failed with STATEMENT_ERROR Exception';
        WHEN OTHER THEN
            RETURN 'S2C Merge SP Failed with OTHER Exception';
END;

-- call stage_sch.copy_csv2stg_orders_sp();
-- call stage_sch.s2c_orders_sp();
-- --------------------------------------------------



