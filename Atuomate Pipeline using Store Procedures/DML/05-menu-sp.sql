use role sysadmin;
use warehouse adhoc_wh;
use database sp_pipeline_db;


CREATE OR REPLACE PROCEDURE stage_sch.copy_csv2stg_menu_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET copy_dml_statement := 
    '
        copy into stage_sch.menu (menuid, restaurantid, itemname, description, price, category, 
                availability, itemtype, createddate, modifieddate,
                _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
        from (
            select 
                t.$1::number as menuid,
                t.$2::text as restaurantid,
                t.$3::text as itemname,
                t.$4::text as description,
                t.$5::text as price,
                t.$6::text as category,
                t.$7::text as availability,
                t.$8::text as itemtype,
                t.$9::text as createddate,
                t.$10::text as modifieddate,
                metadata$filename as _stg_file_name,
                metadata$file_last_modified as _stg_file_load_ts,
                metadata$file_content_key as _stg_file_md5,
                current_timestamp as _copy_data_ts
            from @stage_sch.csv_stg/menu/ t
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

--call stage_sch.copy_csv2stg_menu_sp();
-- --------------------------------------------------

-- stored procedure to run merge statement from 
-- s2c = stage to clean layer data movement
CREATE OR REPLACE PROCEDURE stage_sch.s2c_menu_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO clean_sch.menu AS target
        USING (
            SELECT 
                TRY_CAST(menuid AS INT) AS Menu_ID,
                TRY_CAST(restaurantid AS INT) AS Restaurant_ID_FK,
                TRIM(itemname) AS Item_Name,
                TRIM(description) AS Description,
                TRY_CAST(price AS DECIMAL(10, 2)) AS Price,
                TRIM(category) AS Category,
                CASE 
                    WHEN LOWER(availability) = ''true'' THEN TRUE
                    WHEN LOWER(availability) = ''false'' THEN FALSE
                    ELSE NULL
                END AS Availability,
                TRIM(itemtype) AS Item_Type,
                TRY_CAST(createddate AS TIMESTAMP_NTZ) AS Created_dt,  -- Renamed column
                TRY_CAST(modifieddate AS TIMESTAMP_NTZ) AS Modified_dt, -- Renamed column
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
            FROM stage_sch.menu
        ) AS source
        ON target.Menu_ID = source.Menu_ID
        WHEN MATCHED THEN
            UPDATE SET
                restaurant_id_fk = source.restaurant_id_fk,
                item_name = source.item_name,
                description = source.description,
                price = source.price,
                category = source.category,
                availability = source.availability,
                item_type = source.item_type,
                created_dt = source.created_dt,  
                modified_dt = source.modified_dt,  
                _stg_file_name = source._stg_file_name,
                _stg_file_load_ts = source._stg_file_load_ts,
                _stg_file_md5 = source._stg_file_md5,
                _copy_data_ts = source._copy_data_ts
        WHEN NOT MATCHED THEN
            INSERT (
                Menu_ID,
                Restaurant_ID_FK,
                Item_Name,
                Description,
                Price,
                Category,
                Availability,
                Item_Type,
                Created_dt, 
                Modified_dt,  
                _STG_FILE_NAME,
                _STG_FILE_LOAD_TS,
                _STG_FILE_MD5,
                _COPY_DATA_TS
            )
            VALUES (
                source.Menu_ID,
                source.Restaurant_ID_FK,
                source.Item_Name,
                source.Description,
                source.Price,
                source.Category,
                source.Availability,
                source.Item_Type,
                source.Created_dt,  
                source.Modified_dt,  
                source._stg_file_name,
                source._stg_file_load_ts,
                source._stg_file_md5,
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
-- call stage_sch.s2c_menu_sp();
-- --------------------------------------------------


-- merge statement to populate dimension table
-- c2c = clean to consumption layer data movement
CREATE OR REPLACE PROCEDURE clean_sch.c2c_menu_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN
    -- defining the sql statement as local variable 
    LET merge_dml_statement := 
    '
        MERGE INTO 
            consumption_sch.MENU_DIM AS target
        USING 
            CLEAN_SCH.MENU_STM AS source
        ON 
            target.Menu_ID = source.Menu_ID AND
            target.Restaurant_ID_FK = source.Restaurant_ID_FK AND
            target.Item_Name = source.Item_Name AND
            target.Description = source.Description AND
            target.Price = source.Price AND
            target.Category = source.Category AND
            target.Availability = source.Availability AND
            target.Item_Type = source.Item_Type
        WHEN MATCHED 
            AND source.METADATA$ACTION = ''DELETE'' 
            AND source.METADATA$ISUPDATE = TRUE THEN
            -- Update the existing record to close its validity period
            UPDATE SET 
                target.EFF_END_DATE = CURRENT_TIMESTAMP(),
                target.IS_CURRENT = FALSE
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = ''INSERT'' 
            AND source.METADATA$ISUPDATE = TRUE THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                Menu_Dim_HK,               -- Hash key
                Menu_ID,
                Restaurant_ID_FK,
                Item_Name,
                Description,
                Price,
                Category,
                Availability,
                Item_Type,
                EFF_START_DATE,
                EFF_END_DATE,
                IS_CURRENT
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.Menu_ID, source.Restaurant_ID_FK, 
                    source.Item_Name, source.Description, source.Price, 
                    source.Category, source.Availability, source.Item_Type))),  -- Hash key
                source.Menu_ID,
                source.Restaurant_ID_FK,
                source.Item_Name,
                source.Description,
                source.Price,
                source.Category,
                source.Availability,
                source.Item_Type,
                CURRENT_TIMESTAMP(),       -- Effective start date
                NULL,                      -- Effective end date (NULL for current record)
                TRUE                       -- IS_CURRENT = TRUE for new record
            )
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = ''INSERT'' 
            AND source.METADATA$ISUPDATE = FALSE THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                Menu_Dim_HK,               -- Hash key
                Menu_ID,
                Restaurant_ID_FK,
                Item_Name,
                Description,
                Price,
                Category,
                Availability,
                Item_Type,
                EFF_START_DATE,
                EFF_END_DATE,
                IS_CURRENT
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.Menu_ID, source.Restaurant_ID_FK, 
                    source.Item_Name, source.Description, source.Price, 
                    source.Category, source.Availability, source.Item_Type))),  -- Hash key
                source.Menu_ID,
                source.Restaurant_ID_FK,
                source.Item_Name,
                source.Description,
                source.Price,
                source.Category,
                source.Availability,
                source.Item_Type,
                CURRENT_TIMESTAMP(),       -- Effective start date
                NULL,                      -- Effective end date (NULL for current record)
                TRUE                       -- IS_CURRENT = TRUE for new record
            )'
    ;

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

--call clean_sch.c2c_menu_sp();
-- --------------------------------------------------