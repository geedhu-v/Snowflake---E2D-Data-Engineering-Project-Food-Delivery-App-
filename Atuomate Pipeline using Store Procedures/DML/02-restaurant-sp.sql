use role sysadmin;
use warehouse adhoc_wh;
use database sp_pipeline_db;


CREATE OR REPLACE PROCEDURE stage_sch.copy_csv2stg_restaurant_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET copy_dml_statement := 
    '
        copy into stage_sch.restaurant (restaurantid, name, cuisinetype, pricing_for_2, restaurant_phone, 
                      operatinghours, locationid, activeflag, openstatus, 
                      locality, restaurant_address, latitude, longitude, 
                      createddate, modifieddate, 
                      _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
        from (
            select 
                t.$1::number as restaurantid,        -- restaurantid as the first column
                t.$2::text as name,
                t.$3::text as cuisinetype,
                t.$4::text as pricing_for_2,
                t.$5::text as restaurant_phone,
                t.$6::text as operatinghours,
                t.$7::text as locationid,
                t.$8::text as activeflag,
                t.$9::text as openstatus,
                t.$10::text as locality,
                t.$11::text as restaurant_address,
                t.$12::text as latitude,
                t.$13::text as longitude,
                t.$14::text as createddate,
                t.$15::text as modifieddate,
                -- audit columns for tracking & debugging
                metadata$filename as _stg_file_name,
                metadata$file_last_modified as _stg_file_load_ts,
                metadata$file_content_key as _stg_file_md5,
                current_timestamp() as _copy_data_ts
            from @stage_sch.csv_stg/restaurant/ t
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

--call stage_sch.copy_csv2stg_restaurant_sp();

-- stored procedure to run merge statement from 
-- s2c = stage to clean layer data movement
CREATE OR REPLACE PROCEDURE stage_sch.s2c_restaurant_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO clean_sch.restaurant AS target
        USING (
            SELECT 
                try_cast(restaurantid AS number) AS restaurant_id,
                try_cast(name AS string) AS name,
                try_cast(cuisinetype AS string) AS cuisine_type,
                try_cast(pricing_for_2 AS number(10, 2)) AS pricing_for_two,
                try_cast(restaurant_phone AS string) AS restaurant_phone,
                try_cast(operatinghours AS string) AS operating_hours,
                try_cast(locationid AS number) AS location_id_fk,
                try_cast(activeflag AS string) AS active_flag,
                try_cast(openstatus AS string) AS open_status,
                try_cast(locality AS string) AS locality,
                try_cast(restaurant_address AS string) AS restaurant_address,
                try_cast(latitude AS number(9, 6)) AS latitude,
                try_cast(longitude AS number(9, 6)) AS longitude,
                common.format_timestamp_with_f9(createddate) AS created_dt,
                common.format_timestamp_with_f9(modifieddate) AS modified_dt,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5
            FROM 
                stage_sch.restaurant_stm
        ) AS source
        ON target.restaurant_id = source.restaurant_id
        WHEN MATCHED THEN 
            UPDATE SET 
                target.name = source.name,
                target.cuisine_type = source.cuisine_type,
                target.pricing_for_two = source.pricing_for_two,
                target.restaurant_phone = source.restaurant_phone,
                target.operating_hours = source.operating_hours,
                target.location_id_fk = source.location_id_fk,
                target.active_flag = source.active_flag,
                target.open_status = source.open_status,
                target.locality = source.locality,
                target.restaurant_address = source.restaurant_address,
                target.latitude = source.latitude,
                target.longitude = source.longitude,
                target.created_dt = source.created_dt,
                target.modified_dt = source.modified_dt,
                target._stg_file_name = source._stg_file_name,
                target._stg_file_load_ts = source._stg_file_load_ts,
                target._stg_file_md5 = source._stg_file_md5
        WHEN NOT MATCHED THEN 
            INSERT (
                restaurant_id,
                name,
                cuisine_type,
                pricing_for_two,
                restaurant_phone,
                operating_hours,
                location_id_fk,
                active_flag,
                open_status,
                locality,
                restaurant_address,
                latitude,
                longitude,
                created_dt,
                modified_dt,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5
            )
            VALUES (
                source.restaurant_id,
                source.name,
                source.cuisine_type,
                source.pricing_for_two,
                source.restaurant_phone,
                source.operating_hours,
                source.location_id_fk,
                source.active_flag,
                source.open_status,
                source.locality,
                source.restaurant_address,
                source.latitude,
                source.longitude,
                source.created_dt,
                source.modified_dt,
                source._stg_file_name,
                source._stg_file_load_ts,
                source._stg_file_md5
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
-- call stage_sch.s2c_restaurant_sp();

-- merge statement to populate dimension table
-- c2c = clean to consumption layer data movement
CREATE OR REPLACE PROCEDURE clean_sch.c2c_restaurant_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO 
            CONSUMPTION_SCH.RESTAURANT_DIM AS target
        USING 
            CLEAN_SCH.RESTAURANT_STM AS source
        ON 
            target.RESTAURANT_ID = source.RESTAURANT_ID AND 
            target.NAME = source.NAME AND 
            target.CUISINE_TYPE = source.CUISINE_TYPE AND 
            target.PRICING_FOR_TWO = source.PRICING_FOR_TWO AND 
            target.RESTAURANT_PHONE = source.RESTAURANT_PHONE AND 
            target.OPERATING_HOURS = source.OPERATING_HOURS AND 
            target.LOCATION_ID_FK = source.LOCATION_ID_FK AND 
            target.ACTIVE_FLAG = source.ACTIVE_FLAG AND 
            target.OPEN_STATUS = source.OPEN_STATUS AND 
            target.LOCALITY = source.LOCALITY AND 
            target.RESTAURANT_ADDRESS = source.RESTAURANT_ADDRESS AND 
            target.LATITUDE = source.LATITUDE AND 
            target.LONGITUDE = source.LONGITUDE
        WHEN MATCHED 
            AND source.METADATA$ACTION = ''DELETE'' AND source.METADATA$ISUPDATE = TRUE THEN
            -- Update the existing record to close its validity period
            UPDATE SET 
                target.EFF_END_DATE = CURRENT_TIMESTAMP(),
                target.IS_CURRENT = FALSE
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = ''INSERT'' AND source.METADATA$ISUPDATE = TRUE THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                RESTAURANT_HK,
                RESTAURANT_ID,
                NAME,
                CUISINE_TYPE,
                PRICING_FOR_TWO,
                RESTAURANT_PHONE,
                OPERATING_HOURS,
                LOCATION_ID_FK,
                ACTIVE_FLAG,
                OPEN_STATUS,
                LOCALITY,
                RESTAURANT_ADDRESS,
                LATITUDE,
                LONGITUDE,
                EFF_START_DATE,
                EFF_END_DATE,
                IS_CURRENT
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.RESTAURANT_ID, source.NAME, source.CUISINE_TYPE, 
                    source.PRICING_FOR_TWO, source.RESTAURANT_PHONE, source.OPERATING_HOURS, 
                    source.LOCATION_ID_FK, source.ACTIVE_FLAG, source.OPEN_STATUS, source.LOCALITY, 
                    source.RESTAURANT_ADDRESS, source.LATITUDE, source.LONGITUDE))),
                source.RESTAURANT_ID,
                source.NAME,
                source.CUISINE_TYPE,
                source.PRICING_FOR_TWO,
                source.RESTAURANT_PHONE,
                source.OPERATING_HOURS,
                source.LOCATION_ID_FK,
                source.ACTIVE_FLAG,
                source.OPEN_STATUS,
                source.LOCALITY,
                source.RESTAURANT_ADDRESS,
                source.LATITUDE,
                source.LONGITUDE,
                CURRENT_TIMESTAMP(),
                NULL,
                TRUE
            )
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = ''INSERT'' AND source.METADATA$ISUPDATE = FALSE THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                RESTAURANT_HK,
                RESTAURANT_ID,
                NAME,
                CUISINE_TYPE,
                PRICING_FOR_TWO,
                RESTAURANT_PHONE,
                OPERATING_HOURS,
                LOCATION_ID_FK,
                ACTIVE_FLAG,
                OPEN_STATUS,
                LOCALITY,
                RESTAURANT_ADDRESS,
                LATITUDE,
                LONGITUDE,
                EFF_START_DATE,
                EFF_END_DATE,
                IS_CURRENT
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.RESTAURANT_ID, source.NAME, source.CUISINE_TYPE, 
                    source.PRICING_FOR_TWO, source.RESTAURANT_PHONE, source.OPERATING_HOURS, 
                    source.LOCATION_ID_FK, source.ACTIVE_FLAG, source.OPEN_STATUS, source.LOCALITY, 
                    source.RESTAURANT_ADDRESS, source.LATITUDE, source.LONGITUDE))),
                source.RESTAURANT_ID,
                source.NAME,
                source.CUISINE_TYPE,
                source.PRICING_FOR_TWO,
                source.RESTAURANT_PHONE,
                source.OPERATING_HOURS,
                source.LOCATION_ID_FK,
                source.ACTIVE_FLAG,
                source.OPEN_STATUS,
                source.LOCALITY,
                source.RESTAURANT_ADDRESS,
                source.LATITUDE,
                source.LONGITUDE,
                CURRENT_TIMESTAMP(),
                NULL,
                TRUE
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

--call clean_sch.c2c_restaurant_sp();

