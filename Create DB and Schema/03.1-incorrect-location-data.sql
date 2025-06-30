use role sysadmin;
use warehouse adhoc_wh;
use schema sandbox.stage_sch;

-- Error files
list @stage_sch.csv_stg/delta/location/;

-- run select statement and check stream data.
select * from stage_sch.location;
select * from stage_sch.location_stm;

-- run copy command with specific file
-- delta-day03-invalid-delimiter.csv
copy into stage_sch.location (locationid, city, state, zipcode, activeflag, 
                    createddate, modifieddate, _stg_file_name, 
                    _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::number as locationid,
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
    from @stage_sch.csv_stg/delta/location/delta-day03-invalid-delimiter.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

MERGE INTO clean_sch.restaurant_location AS target
USING (
    SELECT 
        CAST(LocationID AS NUMBER) AS Location_ID,
        CAST(City AS STRING) AS City,
        CASE 
            WHEN CAST(City AS STRING) = 'Delhi' THEN 'New Delhi'
            ELSE CAST(City AS STRING)
        END AS City,
        CASE 
            WHEN CAST(State AS STRING) = 'Delhi' THEN 'New Delhi'
            ELSE CAST(State AS STRING)
        END AS State,
        -- State Code Mapping
        CASE 
            WHEN State = 'Delhi' THEN 'DL'
            WHEN State = 'New Delhi' THEN 'DL'
            WHEN State = 'Maharashtra' THEN 'MH'
            WHEN State = 'Uttar Pradesh' THEN 'UP'
            WHEN State = 'Gujarat' THEN 'GJ'
            WHEN State = 'Rajasthan' THEN 'RJ'
            WHEN State = 'Kerala' THEN 'KL'
            WHEN State = 'Punjab' THEN 'PB'
            WHEN State = 'Karnataka' THEN 'KA'
            WHEN State = 'Madhya Pradesh' THEN 'MP'
            WHEN State = 'Odisha' THEN 'OR'
            WHEN State = 'Chandigarh' THEN 'CH'
            WHEN State = 'West Bengal' THEN 'WB'
            WHEN State = 'Sikkim' THEN 'SK'
            WHEN State = 'Andhra Pradesh' THEN 'AP'
            WHEN State = 'Assam' THEN 'AS'
            WHEN State = 'Jammu and Kashmir' THEN 'JK'
            WHEN State = 'Puducherry' THEN 'PY'
            WHEN State = 'Uttarakhand' THEN 'UK'
            WHEN State = 'Himachal Pradesh' THEN 'HP'
            WHEN State = 'Tamil Nadu' THEN 'TN'
            WHEN State = 'Goa' THEN 'GA'
            WHEN State = 'Telangana' THEN 'TG'
            WHEN State = 'Chhattisgarh' THEN 'CG'
            WHEN State = 'Jharkhand' THEN 'JH'
            WHEN State = 'Bihar' THEN 'BR'
            ELSE NULL
        END AS state_code,
        CASE 
            WHEN State IN ('Delhi', 'New Delhi','Chandigarh', 'Puducherry', 'Jammu and Kashmir', 'Lakshadweep','Ladakh','Dadra and Nagar Haveli and Daman and Diu') THEN 'Y'
            ELSE 'N'
        END AS is_union_territory,
        CASE 
            WHEN (State = 'Delhi' AND City = 'New Delhi') THEN TRUE
            WHEN (State = 'Maharashtra' AND City = 'Mumbai') THEN TRUE
            WHEN (State = 'Bhopal' AND City = 'Madhya Pradesh') THEN TRUE
            WHEN (State = 'Karnataka' AND City = 'Bangalore') THEN TRUE
            WHEN (State = 'Goa' AND City = 'Panaji') THEN TRUE
            WHEN (State = 'Telangana' AND City = 'Hyderabad') THEN TRUE
            WHEN (State = 'Rajasthan' AND City = 'Jaipur') THEN TRUE
            WHEN (State = 'Tamil Nadu' AND City = 'Chennai') THEN TRUE
            -- Other conditions for capital cities
            ELSE FALSE
        END AS capital_city_flag,
        CASE 
            WHEN City IN ('Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad') THEN 'Tier-1'
            WHEN City IN ('Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara', 'Coimbatore', 
                          'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati', 'Chandigarh') THEN 'Tier-2'
            ELSE 'Tier-3'
        END AS city_tier,
        CAST(ZipCode AS STRING) AS Zip_Code,
        CAST(ActiveFlag AS STRING) AS Active_Flag,
        TO_TIMESTAMP_TZ(CreatedDate, 'YYYY-MM-DD HH24:MI:SS') AS created_ts,
        TO_TIMESTAMP_TZ(ModifiedDate, 'YYYY-MM-DD HH24:MI:SS') AS modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts
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
    );

-- clean up script to remove corrupt data. 
create or replace temp table common.location_tmp 
    as 
    select * from stage_sch.location_stm;

delete from stage_sch.location where locationid like '%|%';


-- ----------------------------------------------------
-- Second invalid file scenario.
-- ----------------------------------------------------

-- rerun the stream object
-- run select statement and check stream data.
select * from stage_sch.location;
select * from stage_sch.location_stm;

-- run copy command with delta-day04-invalid-files.csv
copy into stage_sch.location (locationid, city, state, zipcode, activeflag, 
                    createddate, modifieddate, _stg_file_name, 
                    _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::number as locationid,
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
    from @stage_sch.csv_stg/delta/location/delta-day04-invalid-files.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;


MERGE INTO clean_sch.restaurant_location AS target
USING (
    SELECT 
        CAST(LocationID AS NUMBER) AS Location_ID,
        CAST(City AS STRING) AS City,
        CASE 
            WHEN CAST(City AS STRING) = 'Delhi' THEN 'New Delhi'
            ELSE CAST(City AS STRING)
        END AS City,
        CASE 
            WHEN CAST(State AS STRING) = 'Delhi' THEN 'New Delhi'
            ELSE CAST(State AS STRING)
        END AS State,
        -- State Code Mapping
        CASE 
            WHEN State = 'Delhi' THEN 'DL'
            WHEN State = 'New Delhi' THEN 'DL'
            WHEN State = 'Maharashtra' THEN 'MH'
            WHEN State = 'Uttar Pradesh' THEN 'UP'
            WHEN State = 'Gujarat' THEN 'GJ'
            WHEN State = 'Rajasthan' THEN 'RJ'
            WHEN State = 'Kerala' THEN 'KL'
            WHEN State = 'Punjab' THEN 'PB'
            WHEN State = 'Karnataka' THEN 'KA'
            WHEN State = 'Madhya Pradesh' THEN 'MP'
            WHEN State = 'Odisha' THEN 'OR'
            WHEN State = 'Chandigarh' THEN 'CH'
            WHEN State = 'West Bengal' THEN 'WB'
            WHEN State = 'Sikkim' THEN 'SK'
            WHEN State = 'Andhra Pradesh' THEN 'AP'
            WHEN State = 'Assam' THEN 'AS'
            WHEN State = 'Jammu and Kashmir' THEN 'JK'
            WHEN State = 'Puducherry' THEN 'PY'
            WHEN State = 'Uttarakhand' THEN 'UK'
            WHEN State = 'Himachal Pradesh' THEN 'HP'
            WHEN State = 'Tamil Nadu' THEN 'TN'
            WHEN State = 'Goa' THEN 'GA'
            WHEN State = 'Telangana' THEN 'TG'
            WHEN State = 'Chhattisgarh' THEN 'CG'
            WHEN State = 'Jharkhand' THEN 'JH'
            WHEN State = 'Bihar' THEN 'BR'
            ELSE NULL
        END AS state_code,
        CASE 
            WHEN State IN ('Delhi', 'New Delhi','Chandigarh', 'Puducherry', 'Jammu and Kashmir', 'Lakshadweep','Ladakh','Dadra and Nagar Haveli and Daman and Diu') THEN 'Y'
            ELSE 'N'
        END AS is_union_territory,
        CASE 
            WHEN (State = 'Delhi' AND City = 'New Delhi') THEN TRUE
            WHEN (State = 'Maharashtra' AND City = 'Mumbai') THEN TRUE
            WHEN (State = 'Bhopal' AND City = 'Madhya Pradesh') THEN TRUE
            WHEN (State = 'Karnataka' AND City = 'Bangalore') THEN TRUE
            WHEN (State = 'Goa' AND City = 'Panaji') THEN TRUE
            WHEN (State = 'Telangana' AND City = 'Hyderabad') THEN TRUE
            WHEN (State = 'Rajasthan' AND City = 'Jaipur') THEN TRUE
            WHEN (State = 'Tamil Nadu' AND City = 'Chennai') THEN TRUE
            -- Other conditions for capital cities
            ELSE FALSE
        END AS capital_city_flag,
        CASE 
            WHEN City IN ('Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad') THEN 'Tier-1'
            WHEN City IN ('Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara', 'Coimbatore', 
                          'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati', 'Chandigarh') THEN 'Tier-2'
            ELSE 'Tier-3'
        END AS city_tier,
        CAST(ZipCode AS STRING) AS Zip_Code,
        CAST(ActiveFlag AS STRING) AS Active_Flag,
        TO_TIMESTAMP_TZ(CreatedDate, 'YYYY-MM-DD HH24:MI:SS') AS created_ts,
        TO_TIMESTAMP_TZ(ModifiedDate, 'YYYY-MM-DD HH24:MI:SS') AS modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts
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
    );


-- clean up script to remove corrupt data. 
create or replace temp table common.location_tmp 
    as 
    select * from stage_sch.location_stm;

delete from stage_sch.location where locationid like '%junk%';


-- apply the casting to location id and see the result.

copy into stage_sch.location (locationid, city, state, zipcode, activeflag, 
                    createddate, modifieddate, _stg_file_name, 
                    _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::number as locationid,
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
    from @stage_sch.csv_stg/delta/location/delta-day03-invalid-delimiter.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
force = true
on_error = continue;

