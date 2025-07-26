use role sysadmin;
use warehouse adhoc_wh;
use database sp_pipeline_db;


CREATE OR REPLACE PROCEDURE stage_sch.copy_csv2stg_customer_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET copy_dml_statement := 
    '
        copy into  stage_sch.customer (customerid, name, mobile, email, loginbyusing, gender, dob, anniversary, 
                    preferences, createddate, modifieddate, 
                    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
        from (
            select 
                t.$1::number as customerid,
                t.$2::text as name,
                t.$3::text as mobile,
                t.$4::text as email,
                t.$5::text as loginbyusing,
                t.$6::text as gender,
                t.$7::text as dob,
                t.$8::text as anniversary,
                t.$9::text as preferences,
                t.$10::text as createddate,
                t.$11::text as modifieddate,
                metadata$filename as _stg_file_name,
                metadata$file_last_modified as _stg_file_load_ts,
                metadata$file_content_key as _stg_file_md5,
                current_timestamp as _copy_data_ts
            from @stage_sch.csv_stg/customer/ t
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

-- run copy command to load the data into stage-customer table.
--call stage_sch.copy_csv2stg_customer_sp();

-- stored procedure to run merge statement from 
-- s2c = stage to clean layer data movement
CREATE OR REPLACE PROCEDURE stage_sch.s2c_customer_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO CLEAN_SCH.CUSTOMER AS target USING 
        (
            SELECT 
                CUSTOMERID::STRING AS CUSTOMER_ID,
                NAME::STRING AS NAME,
                MOBILE::STRING AS MOBILE,
                EMAIL::STRING AS EMAIL,
                LOGINBYUSING::STRING AS LOGIN_BY_USING,
                GENDER::STRING AS GENDER,
                common.format_date(DOB) AS DOB,                     
                common.format_date(ANNIVERSARY) AS ANNIVERSARY,     
                PREFERENCES::STRING AS PREFERENCES,
                common.format_timestamp_with_f6(createddate) AS created_dt,
                common.format_timestamp_with_f6(modifieddate) AS modified_dt, 
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
            FROM STAGE_SCH.CUSTOMER_STM
        ) AS source
        ON target.CUSTOMER_ID = source.CUSTOMER_ID
        WHEN MATCHED THEN
            UPDATE SET 
                target.NAME = source.NAME,
                target.MOBILE = source.MOBILE,
                target.EMAIL = source.EMAIL,
                target.LOGIN_BY_USING = source.LOGIN_BY_USING,
                target.GENDER = source.GENDER,
                target.DOB = source.DOB,
                target.ANNIVERSARY = source.ANNIVERSARY,
                target.PREFERENCES = source.PREFERENCES,
                target.CREATED_DT = source.CREATED_DT,
                target.MODIFIED_DT = source.MODIFIED_DT,
                target._STG_FILE_NAME = source._STG_FILE_NAME,
                target._STG_FILE_LOAD_TS = source._STG_FILE_LOAD_TS,
                target._STG_FILE_MD5 = source._STG_FILE_MD5,
                target._COPY_DATA_TS = source._COPY_DATA_TS
        WHEN NOT MATCHED THEN
            INSERT (
                CUSTOMER_ID,
                NAME,
                MOBILE,
                EMAIL,
                LOGIN_BY_USING,
                GENDER,
                DOB,
                ANNIVERSARY,
                PREFERENCES,
                CREATED_DT,
                MODIFIED_DT,
                _STG_FILE_NAME,
                _STG_FILE_LOAD_TS,
                _STG_FILE_MD5,
                _COPY_DATA_TS
            )
            VALUES (
                source.CUSTOMER_ID,
                source.NAME,
                source.MOBILE,
                source.EMAIL,
                source.LOGIN_BY_USING,
                source.GENDER,
                source.DOB,
                source.ANNIVERSARY,
                source.PREFERENCES,
                source.CREATED_DT,
                source.MODIFIED_DT,
                source._STG_FILE_NAME,
                source._STG_FILE_LOAD_TS,
                source._STG_FILE_MD5,
                source._COPY_DATA_TS
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
--call stage_sch.s2c_customer_sp();

-- merge statement to populate dimension table
-- c2c = clean to consumption layer data movement
CREATE OR REPLACE PROCEDURE clean_sch.c2c_customer_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO 
            CONSUMPTION_SCH.CUSTOMER_DIM AS target
        USING 
            CLEAN_SCH.CUSTOMER_STM AS source
        ON 
            target.CUSTOMER_ID = source.CUSTOMER_ID AND
            target.NAME = source.NAME AND
            target.MOBILE = source.MOBILE AND
            target.EMAIL = source.EMAIL AND
            target.LOGIN_BY_USING = source.LOGIN_BY_USING AND
            target.GENDER = source.GENDER AND
            target.DOB = source.DOB AND
            target.ANNIVERSARY = source.ANNIVERSARY AND
            target.PREFERENCES = source.PREFERENCES
        WHEN MATCHED 
            AND source.METADATA$ACTION = ''DELETE'' AND source.METADATA$ISUPDATE =  TRUE THEN
            -- Update the existing record to close its validity period
            UPDATE SET 
                target.EFF_END_DATE = CURRENT_TIMESTAMP(),
                target.IS_CURRENT = FALSE
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = ''INSERT'' AND source.METADATA$ISUPDATE =  TRUE THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                CUSTOMER_HK,
                CUSTOMER_ID,
                NAME,
                MOBILE,
                EMAIL,
                LOGIN_BY_USING,
                GENDER,
                DOB,
                ANNIVERSARY,
                PREFERENCES,
                EFF_START_DATE,
                EFF_END_DATE,
                IS_CURRENT
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.CUSTOMER_ID, source.NAME, source.MOBILE, 
                    source.EMAIL, source.LOGIN_BY_USING, source.GENDER, source.DOB, 
                    source.ANNIVERSARY, source.PREFERENCES))),
                source.CUSTOMER_ID,
                source.NAME,
                source.MOBILE,
                source.EMAIL,
                source.LOGIN_BY_USING,
                source.GENDER,
                source.DOB,
                source.ANNIVERSARY,
                source.PREFERENCES,
                CURRENT_TIMESTAMP(),
                NULL,
                TRUE
            )
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = ''INSERT'' AND source.METADATA$ISUPDATE =  FALSE THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                CUSTOMER_HK,
                CUSTOMER_ID,
                NAME,
                MOBILE,
                EMAIL,
                LOGIN_BY_USING,
                GENDER,
                DOB,
                ANNIVERSARY,
                PREFERENCES,
                EFF_START_DATE,
                EFF_END_DATE,
                IS_CURRENT
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.CUSTOMER_ID, source.NAME, source.MOBILE, 
                    source.EMAIL, source.LOGIN_BY_USING, source.GENDER, source.DOB, 
                    source.ANNIVERSARY, source.PREFERENCES))),
                source.CUSTOMER_ID,
                source.NAME,
                source.MOBILE,
                source.EMAIL,
                source.LOGIN_BY_USING,
                source.GENDER,
                source.DOB,
                source.ANNIVERSARY,
                source.PREFERENCES,
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

--call clean_sch.c2c_customer_sp();
