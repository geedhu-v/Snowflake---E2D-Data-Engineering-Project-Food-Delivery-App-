
use role sysadmin;
use warehouse adhoc_wh;
use database sp_pipeline_db;


CREATE OR REPLACE PROCEDURE stage_sch.copy_csv2stg_customer_address_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET copy_dml_statement := 
    '
        copy into stage_sch.customeraddress (addressid, customerid, flatno, houseno, floor, building, 
                               landmark, locality,city,pincode, state, coordinates, primaryflag, addresstype, 
                               createddate, modifieddate, 
                               _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
    from (
        select 
            t.$1::number as addressid,
            t.$2::text as customerid,
            t.$3::text as flatno,
            t.$4::text as houseno,
            t.$5::text as floor,
            t.$6::text as building,
            t.$7::text as landmark,
            t.$8::text as locality,
            t.$9::text as city,
            t.$10::text as State,
            t.$11::text as Pincode,
            t.$12::text as coordinates,
            t.$13::text as primaryflag,
            t.$14::text as addresstype,
            t.$15::text as createddate,
            t.$16::text as modifieddate,
            metadata$filename as _stg_file_name,
            metadata$file_last_modified as _stg_file_load_ts,
            metadata$file_content_key as _stg_file_md5,
            current_timestamp as _copy_data_ts
        from @stage_sch.csv_stg/customer-address/ t
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

--call stage_sch.copy_csv2stg_customer_address_sp();

-- stored procedure to run merge statement from 
-- s2c = stage to clean layer data movement
CREATE OR REPLACE PROCEDURE stage_sch.s2c_customer_address_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO clean_sch.customer_address AS clean
        USING (
            SELECT 
                CAST(addressid AS INT) AS address_id,
                CAST(customerid AS INT) AS customer_id_fk,
                flatno AS flat_no,
                houseno AS house_no,
                floor,
                building,
                landmark,
                locality,
                city,
                state,
                pincode,
                coordinates,
                primaryflag AS primary_flag,
                addresstype AS address_type,
                common.format_timestamp_v2(createddate) AS created_date,
                common.format_timestamp_v2(modifieddate) AS modified_date,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
            FROM stage_sch.customeraddress_stm 
        ) AS stage
        ON clean.address_id = stage.address_id
        -- Insert new records
        WHEN NOT MATCHED THEN
            INSERT (
                address_id,
                customer_id_fk,
                flat_no,
                house_no,
                floor,
                building,
                landmark,
                locality,
                city,
                state,
                pincode,
                coordinates,
                primary_flag,
                address_type,
                created_date,
                modified_date,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
            )
            VALUES (
                stage.address_id,
                stage.customer_id_fk,
                stage.flat_no,
                stage.house_no,
                stage.floor,
                stage.building,
                stage.landmark,
                stage.locality,
                stage.city,
                stage.state,
                stage.pincode,
                stage.coordinates,
                stage.primary_flag,
                stage.address_type,
                stage.created_date,
                stage.modified_date,
                stage._stg_file_name,
                stage._stg_file_load_ts,
                stage._stg_file_md5,
                stage._copy_data_ts
            )
        -- Update existing records
        WHEN MATCHED THEN
            UPDATE SET
                clean.flat_no = stage.flat_no,
                clean.house_no = stage.house_no,
                clean.floor = stage.floor,
                clean.building = stage.building,
                clean.landmark = stage.landmark,
                clean.locality = stage.locality,
                clean.city = stage.city,
                clean.state = stage.state,
                clean.pincode = stage.pincode,
                clean.coordinates = stage.coordinates,
                clean.primary_flag = stage.primary_flag,
                clean.address_type = stage.address_type,
                clean.created_date = stage.created_date,
                clean.modified_date = stage.modified_date,
                clean._stg_file_name = stage._stg_file_name,
                clean._stg_file_load_ts = stage._stg_file_load_ts,
                clean._stg_file_md5 = stage._stg_file_md5,
                clean._copy_data_ts = stage._copy_data_ts
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
-- call stage_sch.s2c_customer_address_sp();

-- merge statement to populate dimension table
-- c2c = clean to consumption layer data movement
CREATE OR REPLACE PROCEDURE clean_sch.c2c_customer_address_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO 
            CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM AS target
        USING 
            CLEAN_SCH.CUSTOMER_ADDRESS_STM AS source
        ON 
            target.ADDRESS_ID = source.ADDRESS_ID AND
            target.CUSTOMER_ID_FK = source.CUSTOMER_ID_FK AND
            target.FLAT_NO = source.FLAT_NO AND
            target.HOUSE_NO = source.HOUSE_NO AND
            target.FLOOR = source.FLOOR AND
            target.BUILDING = source.BUILDING AND
            target.LANDMARK = source.LANDMARK AND
            target.LOCALITY = source.LOCALITY AND
            target.CITY = source.CITY AND
            target.STATE = source.STATE AND
            target.PINCODE = source.PINCODE AND
            target.COORDINATES = source.COORDINATES AND
            target.PRIMARY_FLAG = source.PRIMARY_FLAG AND
            target.ADDRESS_TYPE = source.ADDRESS_TYPE
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
                CUSTOMER_ADDRESS_HK,
                ADDRESS_ID,
                CUSTOMER_ID_FK,
                FLAT_NO,
                HOUSE_NO,
                FLOOR,
                BUILDING,
                LANDMARK,
                LOCALITY,
                CITY,
                STATE,
                PINCODE,
                COORDINATES,
                PRIMARY_FLAG,
                ADDRESS_TYPE,
                EFF_START_DATE,
                EFF_END_DATE,
                IS_CURRENT
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.ADDRESS_ID, source.CUSTOMER_ID_FK, source.FLAT_NO, 
                    source.HOUSE_NO, source.FLOOR, source.BUILDING, source.LANDMARK, 
                    source.LOCALITY, source.CITY, source.STATE, source.PINCODE, 
                    source.COORDINATES, source.PRIMARY_FLAG, source.ADDRESS_TYPE))),
                source.ADDRESS_ID,
                source.CUSTOMER_ID_FK,
                source.FLAT_NO,
                source.HOUSE_NO,
                source.FLOOR,
                source.BUILDING,
                source.LANDMARK,
                source.LOCALITY,
                source.CITY,
                source.STATE,
                source.PINCODE,
                source.COORDINATES,
                source.PRIMARY_FLAG,
                source.ADDRESS_TYPE,
                CURRENT_TIMESTAMP(),
                NULL,
                TRUE
            )
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = ''INSERT'' AND source.METADATA$ISUPDATE = FALSE THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                CUSTOMER_ADDRESS_HK,
                ADDRESS_ID,
                CUSTOMER_ID_FK,
                FLAT_NO,
                HOUSE_NO,
                FLOOR,
                BUILDING,
                LANDMARK,
                LOCALITY,
                CITY,
                STATE,
                PINCODE,
                COORDINATES,
                PRIMARY_FLAG,
                ADDRESS_TYPE,
                EFF_START_DATE,
                EFF_END_DATE,
                IS_CURRENT
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.ADDRESS_ID, source.CUSTOMER_ID_FK, source.FLAT_NO, 
                    source.HOUSE_NO, source.FLOOR, source.BUILDING, source.LANDMARK, 
                    source.LOCALITY, source.CITY, source.STATE, source.PINCODE, 
                    source.COORDINATES, source.PRIMARY_FLAG, source.ADDRESS_TYPE))),
                source.ADDRESS_ID,
                source.CUSTOMER_ID_FK,
                source.FLAT_NO,
                source.HOUSE_NO,
                source.FLOOR,
                source.BUILDING,
                source.LANDMARK,
                source.LOCALITY,
                source.CITY,
                source.STATE,
                source.PINCODE,
                source.COORDINATES,
                source.PRIMARY_FLAG,
                source.ADDRESS_TYPE,
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

--call clean_sch.c2c_customer_address_sp();

