
use role sysadmin;
use warehouse adhoc_wh;
use database sp_pipeline_db;


CREATE OR REPLACE PROCEDURE stage_sch.copy_csv2stg_delivery_agent_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET copy_dml_statement := 
    '
        copy into stage_sch.deliveryagent (deliveryagentid, name, phone, vehicletype, locationid, 
                         status, gender, rating, createddate, modifieddate,
                         _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
        from (
            select 
                t.$1::number as deliveryagentid,
                t.$2::text as name,
                t.$3::text as phone,
                t.$4::text as vehicletype,
                t.$5::text as locationid,
                t.$6::text as status,
                t.$7::text as gender,
                t.$8::text as rating,
                t.$9::text as createddate,
                t.$10::text as modifieddate,
                metadata$filename as _stg_file_name,
                metadata$file_last_modified as _stg_file_load_ts,
                metadata$file_content_key as _stg_file_md5,
                current_timestamp as _copy_data_ts
            from @stage_sch.csv_stg/delivery-agent/ t
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

--call stage_sch.copy_csv2stg_delivery_agent_sp();
-- --------------------------------------------------
-- stored procedure to run merge statement from 
-- s2c = stage to clean layer data movement
CREATE OR REPLACE PROCEDURE stage_sch.s2c_delivery_agent_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO clean_sch.delivery_agent AS target
        USING stage_sch.deliveryagent_stm AS source
        ON target.delivery_agent_id = source.deliveryagentid
        WHEN MATCHED THEN
            UPDATE SET
                target.phone = source.phone,
                target.vehicle_type = source.vehicletype,
                target.location_id_fk = TRY_TO_NUMBER(source.locationid),
                target.status = source.status,
                target.gender = source.gender,
                target.rating = TRY_TO_DECIMAL(source.rating,4,2),
                target.created_dt = TRY_TO_TIMESTAMP(source.createddate),
                target.modified_dt = TRY_TO_TIMESTAMP(source.modifieddate),
                target._stg_file_name = source._stg_file_name,
                target._stg_file_load_ts = source._stg_file_load_ts,
                target._stg_file_md5 = source._stg_file_md5,
                target._copy_data_ts = source._copy_data_ts
        WHEN NOT MATCHED THEN
            INSERT (
                delivery_agent_id,
                name,
                phone,
                vehicle_type,
                location_id_fk,
                status,
                gender,
                rating,
                created_dt,
                modified_dt,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
            )
            VALUES (
                TRY_TO_NUMBER(source.deliveryagentid),
                source.name,
                source.phone,
                source.vehicletype,
                TRY_TO_NUMBER(source.locationid),
                source.status,
                source.gender,
                TRY_TO_NUMBER(source.rating),
                TRY_TO_TIMESTAMP(source.createddate),
                TRY_TO_TIMESTAMP(source.modifieddate),
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
-- call stage_sch.s2c_delivery_agent_sp();
-- --------------------------------------------------

-- merge statement to populate dimension table
-- c2c = clean to consumption layer data movement
CREATE OR REPLACE PROCEDURE clean_sch.c2c_delivery_agent_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO consumption_sch.delivery_agent_dim AS target
        USING CLEAN_SCH.delivery_agent_stm AS source
        ON 
            target.delivery_agent_id = source.delivery_agent_id AND
            target.name = source.name AND
            target.phone = source.phone AND
            target.vehicle_type = source.vehicle_type AND
            target.location_id_fk = source.location_id_fk AND
            target.status = source.status AND
            target.gender = source.gender AND
            target.rating = source.rating
        WHEN MATCHED 
            AND source.METADATA$ACTION = ''DELETE'' 
            AND source.METADATA$ISUPDATE = TRUE THEN
            -- Update the existing record to close its validity period
            UPDATE SET 
                target.eff_end_date = CURRENT_TIMESTAMP,
                target.is_current = FALSE
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = ''INSERT'' 
            AND source.METADATA$ISUPDATE = TRUE THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                delivery_agent_hk,        -- Hash key
                delivery_agent_id,
                name,
                phone,
                vehicle_type,
                location_id_fk,
                status,
                gender,
                rating,
                eff_start_date,
                eff_end_date,
                is_current
            )
            VALUES (
                hash(SHA1_HEX(CONCAT(source.delivery_agent_id, source.name, source.phone, 
                    source.vehicle_type, source.location_id_fk, source.status, 
                    source.gender, source.rating))), -- Hash key
                delivery_agent_id,
                source.name,
                source.phone,
                source.vehicle_type,
                location_id_fk,
                source.status,
                source.gender,
                source.rating,
                CURRENT_TIMESTAMP,       -- Effective start date
                NULL,                    -- Effective end date (NULL for current record)
                TRUE                    -- IS_CURRENT = TRUE for new record
            )
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = ''INSERT'' 
            AND source.METADATA$ISUPDATE = FALSE THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                delivery_agent_hk,        -- Hash key
                delivery_agent_id,
                name,
                phone,
                vehicle_type,
                location_id_fk,
                status,
                gender,
                rating,
                eff_start_date,
                eff_end_date,
                is_current
            )
            VALUES (
                hash(SHA1_HEX(CONCAT(source.delivery_agent_id, source.name, source.phone, 
                    source.vehicle_type, source.location_id_fk, source.status,
                    source.gender, source.rating))), -- Hash key
                source.delivery_agent_id,
                source.name,
                source.phone,
                source.vehicle_type,
                source.location_id_fk,
                source.status,
                source.gender,
                source.rating,
                CURRENT_TIMESTAMP,       -- Effective start date
                NULL,                    -- Effective end date (NULL for current record)
                TRUE                   -- IS_CURRENT = TRUE for new record
            )
    ';

    -- run the copy command
    EXECUTE IMMEDIATE merge_dml_statement;

    -- if all goes good, the retunr will be executed
    return 'C2C Merge SP Exeucted without any issue';

    -- in case the excption occurs, it will come to this point 
    EXCEPTION
        WHEN EXPRESSION_ERROR THEN
            RETURN 'C2C Merge SP Failed with EXPRESSION_ERROR Exception';
        WHEN STATEMENT_ERROR THEN
            RETURN 'C2C Merge SP Failed with STATEMENT_ERROR Exception';
        WHEN OTHER THEN
            RETURN 'C2C Merge SP Failed with OTHER Exception';
END;

--call clean_sch.c2c_delivery_agent_sp();
-- --------------------------------------------------

