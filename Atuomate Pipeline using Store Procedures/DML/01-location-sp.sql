use role sysadmin;
use warehouse adhoc_wh;
use schema sp_pipeline_db.stage_sch;

-- csv2s = CSV to stage table
CREATE OR REPLACE PROCEDURE stage_sch.copy_csv2stg_location_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET copy_dml_statement := 
    '
        copy into stage_sch.location (locationid, city, state, zipcode, activeflag, 
                        createddate, modifieddate, _stg_file_name, 
                        _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
        from (
            select 
                t.$1::text as locationid,
                t.$2::text as city,
                t.$3::text as state,
                t.$4::text as zipcode,
                t.$5::text as activeflag,
                t.$6::text as createddate,
                t.$7::text as modifieddate,
                metadata$filename as _stg_file_name,
                metadata$file_last_modified as _stg_file_load_ts,
                metadata$file_content_key as _stg_file_md5,
                current_timestamp as _copy_data_ts
            from @stage_sch.csv_stg/location/ t
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

call stage_sch.copy_csv2stg_location_sp();
select * from table(result_scan());

-- stored procedure to run merge statement from 
-- s2c = stage to clean layer data movement
CREATE OR REPLACE PROCEDURE stage_sch.s2c_location_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO clean_sch.restaurant_location AS target
        USING (
            SELECT 
                cast(locationid as number) as location_id,
                common.get_city(City) AS City,
                common.get_state(State) AS State,
                common.get_state_code(State) AS state_code,
                common.is_union_territory(State) AS is_union_territory,
                common.is_capital_city(State, City) AS capital_city_flag,
                common.get_city_tier(City) AS city_tier,
                cast(zipcode as string) as zip_code,
                cast(activeflag as string) as active_flag,
                common.format_timestamp(createddate) AS created_ts,
                common.format_timestamp(modifieddate) AS modified_ts,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                current_timestamp as _copy_data_ts
            FROM stage_sch.location_stm
        ) AS source
        ON target.Location_ID = source.Location_ID
        WHEN MATCHED AND (
            target.City != source.City OR
            target.State != source.State OR
            target.state_code != source.state_code OR
            target.is_union_territory != source.is_union_territory OR
            target.capital_city_flag != source.capital_city_flag OR
            target.city_tier != source.city_tier OR
            target.Zip_Code != source.Zip_Code OR
            target.Active_Flag != source.Active_Flag OR
            target.modified_ts != source.modified_ts
        ) THEN 
        UPDATE SET 
            target.City = source.City,
            target.State = source.State,
            target.state_code = source.state_code,
            target.is_union_territory = source.is_union_territory,
            target.capital_city_flag = source.capital_city_flag,
            target.city_tier = source.city_tier,
            target.Zip_Code = source.Zip_Code,
            target.Active_Flag = source.Active_Flag,
            target.modified_ts = source.modified_ts,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._copy_data_ts = source._copy_data_ts
        WHEN NOT MATCHED THEN
        INSERT (
            Location_ID,
            City,
            State,
            state_code,
            is_union_territory,
            capital_city_flag,
            city_tier,
            Zip_Code,
            Active_Flag,
            created_ts,
            modified_ts,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _copy_data_ts
        )
        VALUES (
            source.Location_ID,
            source.City,
            source.State,
            source.state_code,
            source.is_union_territory,
            source.capital_city_flag,
            source.city_tier,
            source.Zip_Code,
            source.Active_Flag,
            source.created_ts,
            source.modified_ts,
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
-- call stage_sch.stg2clean_location_sp();

-- merge statement to populate dimension table
-- c2c = clean to consumption layer data movement
CREATE OR REPLACE PROCEDURE clean_sch.c2c_location_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO 
        CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM AS target
            USING 
                CLEAN_SCH.RESTAURANT_LOCATION_STM AS source
            ON 
                target.LOCATION_ID = source.LOCATION_ID and 
                target.ACTIVE_FLAG = source.ACTIVE_FLAG
            WHEN MATCHED 
                AND source.METADATA$ACTION = ''DELETE'' and source.METADATA$ISUPDATE = TRUE THEN
            -- Update the existing record to close its validity period
            UPDATE SET 
                target.EFF_END_DT = CURRENT_TIMESTAMP(),
                target.CURRENT_FLAG = FALSE
            WHEN NOT MATCHED 
                AND source.METADATA$ACTION = ''INSERT'' and source.METADATA$ISUPDATE = TRUE
            THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                RESTAURANT_LOCATION_HK,
                LOCATION_ID,
                CITY,
                STATE,
                STATE_CODE,
                IS_UNION_TERRITORY,
                CAPITAL_CITY_FLAG,
                CITY_TIER,
                ZIP_CODE,
                ACTIVE_FLAG,
                EFF_START_DT,
                EFF_END_DT,
                CURRENT_FLAG
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
                source.LOCATION_ID,
                source.CITY,
                source.STATE,
                source.STATE_CODE,
                source.IS_UNION_TERRITORY,
                source.CAPITAL_CITY_FLAG,
                source.CITY_TIER,
                source.ZIP_CODE,
                source.ACTIVE_FLAG,
                CURRENT_TIMESTAMP(),
                NULL,
                TRUE
            )
            WHEN NOT MATCHED AND 
            source.METADATA$ACTION = ''INSERT'' and source.METADATA$ISUPDATE = FALSE THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                RESTAURANT_LOCATION_HK,
                LOCATION_ID,
                CITY,
                STATE,
                STATE_CODE,
                IS_UNION_TERRITORY,
                CAPITAL_CITY_FLAG,
                CITY_TIER,
                ZIP_CODE,
                ACTIVE_FLAG,
                EFF_START_DT,
                EFF_END_DT,
                CURRENT_FLAG
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
                source.LOCATION_ID,
                source.CITY,
                source.STATE,
                source.STATE_CODE,
                source.IS_UNION_TERRITORY,
                source.CAPITAL_CITY_FLAG,
                source.CITY_TIER,
                source.ZIP_CODE,
                source.ACTIVE_FLAG,
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

--call clean_sch.c2c_location_sp();
