
use role sysadmin;
use warehouse adhoc_wh;
use database sp_pipeline_db;


CREATE OR REPLACE PROCEDURE stage_sch.copy_csv2stg_order_items_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET copy_dml_statement := 
    '
        copy into stage_sch.orderitem (orderitemid, orderid, menuid, quantity, price, 
                     subtotal, createddate, modifieddate,
                     _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
        from (
            select 
                t.$1::number as orderitemid,
                t.$2::text as orderid,
                t.$3::text as menuid,
                t.$4::text as quantity,
                t.$5::text as price,
                t.$6::text as subtotal,
                t.$7::text as createddate,
                t.$8::text as modifieddate,
                metadata$filename as _stg_file_name,
                metadata$file_last_modified as _stg_file_load_ts,
                metadata$file_content_key as _stg_file_md5,
                current_timestamp as _copy_data_ts
            from @stage_sch.csv_stg/order-items/ t
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

--call stage_sch.copy_csv2stg_order_items_sp();
-- --------------------------------------------------

-- stored procedure to run merge statement from 
-- s2c = stage to clean layer data movement
CREATE OR REPLACE PROCEDURE stage_sch.s2c_order_items_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO clean_sch.order_item AS target
        USING stage_sch.orderitem_stm AS source
        ON  
            target.order_item_id = source.orderitemid and
            target.order_id_fk = source.orderid and
            target.menu_id_fk = source.menuid
        WHEN MATCHED THEN
            -- Update the existing record with new data
            UPDATE SET 
                target.quantity = source.quantity,
                target.price = source.price,
                target.subtotal = source.subtotal,
                target.created_dt = source.createddate,
                target.modified_dt = source.modifieddate,
                target._stg_file_name = source._stg_file_name,
                target._stg_file_load_ts = source._stg_file_load_ts,
                target._stg_file_md5 = source._stg_file_md5,
                target._copy_data_ts = source._copy_data_ts
        WHEN NOT MATCHED THEN
            -- Insert new record if no match is found
            INSERT (
                order_item_id,
                order_id_fk,
                menu_id_fk,
                quantity,
                price,
                subtotal,
                created_dt,
                modified_dt,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
            )
            VALUES (
                source.orderitemid,
                source.orderid,
                source.menuid,
                source.quantity,
                source.price,
                source.subtotal,
                source.createddate,
                source.modifieddate,
                source._stg_file_name,
                source._stg_file_load_ts,
                source._stg_file_md5,
                CURRENT_TIMESTAMP()
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

-- call stage_sch.copy_csv2stg_order_items_sp();
-- call stage_sch.s2c_order_items_sp();
-- --------------------------------------------------




