use role sysadmin;
use warehouse adhoc_wh;
use database sp_pipeline_db;


CREATE OR REPLACE PROCEDURE stage_sch.copy_csv2stg_delivery_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET copy_dml_statement := 
    '
        copy into stage_sch.delivery (deliveryid,orderid, deliveryagentid, deliverystatus, 
                    estimatedtime, addressid, deliverydate, createddate, 
                    modifieddate, _stg_file_name, _stg_file_load_ts, 
                    _stg_file_md5, _copy_data_ts)
        from (
            select 
                t.$1::number as deliveryid,
                t.$2::text as orderid,
                t.$3::text as deliveryagentid,
                t.$4::text as deliverystatus,
                t.$5::text as estimatedtime,
                t.$6::text as addressid,
                t.$7::text as deliverydate,
                t.$8::text as createddate,
                t.$9::text as modifieddate,
                metadata$filename as _stg_file_name,
                metadata$file_last_modified as _stg_file_load_ts,
                metadata$file_content_key as _stg_file_md5,
                current_timestamp as _copy_data_ts
            from @stage_sch.csv_stg/delivery/ t
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

--call stage_sch.copy_csv2stg_delivery_sp();

-- --------------------------------------------------
-- stored procedure to run merge statement from 
-- s2c = stage to clean layer data movement
CREATE OR REPLACE PROCEDURE stage_sch.s2c_delivery_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO 
        clean_sch.delivery AS target
        USING 
            stage_sch.delivery_stm AS source
        ON 
            target.delivery_id = TO_NUMBER(source.deliveryid) and
            target.order_id_fk = TO_NUMBER(source.orderid) and
            target.delivery_agent_id_fk = TO_NUMBER(source.deliveryagentid)
        WHEN MATCHED THEN
            -- Update the existing record with the latest data
            UPDATE SET
                delivery_status = source.deliverystatus,
                estimated_time = source.estimatedtime,
                customer_address_id_fk = TO_NUMBER(source.addressid),
                delivery_date = TO_TIMESTAMP(source.deliverydate),
                created_date = TO_TIMESTAMP(source.createddate),
                modified_date = TO_TIMESTAMP(source.modifieddate),
                _stg_file_name = source._stg_file_name,
                _stg_file_load_ts = source._stg_file_load_ts,
                _stg_file_md5 = source._stg_file_md5,
                _copy_data_ts = source._copy_data_ts
        WHEN NOT MATCHED THEN
            -- Insert new record if no match is found
            INSERT (
                delivery_id,
                order_id_fk,
                delivery_agent_id_fk,
                delivery_status,
                estimated_time,
                customer_address_id_fk,
                delivery_date,
                created_date,
                modified_date,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
            )
            VALUES (
                TO_NUMBER(source.deliveryid),
                TO_NUMBER(source.orderid),
                TO_NUMBER(source.deliveryagentid),
                source.deliverystatus,
                source.estimatedtime,
                TO_NUMBER(source.addressid),
                TO_TIMESTAMP(source.deliverydate),
                TO_TIMESTAMP(source.createddate),
                TO_TIMESTAMP(source.modifieddate),
                source._stg_file_name,
                source._stg_file_load_ts,
                source._stg_file_md5,
                source._copy_data_ts
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

call stage_sch.copy_csv2stg_delivery_sp();
call stage_sch.s2c_delivery_sp();
-- 
--select * from CLEAN_SCH.delivery_stm ;
-- --------------------------------------------------

