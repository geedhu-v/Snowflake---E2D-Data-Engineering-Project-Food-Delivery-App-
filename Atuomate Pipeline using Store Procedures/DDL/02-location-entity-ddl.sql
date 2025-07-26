use role sysadmin;
use warehouse adhoc_wh;
use schema sp_pipeline_db.stage_sch;

-- Creating stage level location table
create or replace table stage_sch.location (
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createddate text,
    modifieddate text,
    -- audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the location stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.'
;

-- create a append only stream object on stage location table.
create or replace stream stage_sch.location_stm 
on table stage_sch.location
append_only = true
comment = 'This is the append-only stream object on location table that gets delta data based on changes';


-- create restaurant location clean layer table.
create or replace table clean_sch.restaurant_location (
    restaurant_location_sk number autoincrement primary key,
    location_id number not null unique,
    city string(100) not null,
    state string(100) not null,
    state_code string(2) not null,
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier text(6),
    zip_code string(10) not null,
    active_flag string(10) not null,
    created_ts timestamp_tz not null,
    modified_ts timestamp_tz,
    
    -- additional audit columns
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
)
comment = 'Location entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- create standard stream object
create or replace stream clean_sch.restaurant_location_stm 
on table clean_sch.restaurant_location
comment = 'This is a standard stream object on the location table to track insert, update, and delete changes';

-- create dim table
create or replace table consumption_sch.restaurant_location_dim (
    restaurant_location_hk NUMBER primary key,                      -- hash key for the dimension
    location_id number(38,0) not null,                  -- business key
    city varchar(100) not null,                         -- city
    state varchar(100) not null,                        -- state
    state_code varchar(2) not null,                     -- state code
    is_union_territory boolean not null default false,   -- union territory flag
    capital_city_flag boolean not null default false,     -- capital city flag
    city_tier varchar(6),                               -- city tier
    zip_code varchar(10) not null,                      -- zip code
    active_flag varchar(10) not null,                   -- active flag (indicating current record)
    eff_start_dt timestamp_tz(9) not null,              -- effective start date for scd2
    eff_end_dt timestamp_tz(9),                         -- effective end date for scd2
    current_flag boolean not null default true         -- indicator of the current record
)
comment = 'Dimension table for restaurant location with scd2 (slowly changing dimension) enabled and hashkey as surrogate key';


-- standard UDFs
CREATE OR REPLACE FUNCTION common.get_city(City STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN City = 'Delhi' THEN 'New Delhi'
        ELSE City
    END
$$;

CREATE OR REPLACE FUNCTION common.get_state(State STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
   CASE 
        WHEN State = 'Delhi' THEN 'New Delhi'
        ELSE State
    END
$$;

CREATE OR REPLACE FUNCTION common.get_state_code(State STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
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
        WHEN State = 'Jammu & Kashmir' THEN 'JK'
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
    END
$$;

CREATE OR REPLACE FUNCTION common.is_union_territory(State STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN State IN ('Delhi', 'New Delhi', 'Chandigarh', 'Puducherry', 
                       'Jammu and Kashmir', 'Jammu & Kashmir','Lakshadweep', 'Ladakh', 
                       'Dadra and Nagar Haveli and Daman and Diu') THEN 'Y'
        ELSE 'N'
    END
$$;
   
    
CREATE OR REPLACE FUNCTION common.is_capital_city(State STRING, City STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    CASE 
        WHEN (State = 'Delhi' AND City = 'New Delhi') THEN TRUE
        WHEN (State = 'Maharashtra' AND City = 'Mumbai') THEN TRUE
        WHEN (State = 'Madhya Pradesh' AND City = 'Bhopal') THEN TRUE
        WHEN (State = 'Karnataka' AND City = 'Bangalore') THEN TRUE
        WHEN (State = 'Goa' AND City = 'Panaji') THEN TRUE
        WHEN (State = 'Telangana' AND City = 'Hyderabad') THEN TRUE
        WHEN (State = 'Rajasthan' AND City = 'Jaipur') THEN TRUE
        WHEN (State = 'Tamil Nadu' AND City = 'Chennai') THEN TRUE
        -- Add other conditions for capital cities as needed
        ELSE FALSE
    END
$$;


CREATE OR REPLACE FUNCTION common.get_city_tier(City STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN City IN ('Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad') THEN 'Tier-1'
        WHEN City IN ('Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara', 
                      'Coimbatore', 'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati', 
                      'Chandigarh') THEN 'Tier-2'
        ELSE 'Tier-3'
    END
$$;

CREATE OR REPLACE FUNCTION common.format_date(dt_str STRING)
RETURNS DATE
LANGUAGE SQL
AS
$$
    TRY_TO_DATE(dt_str, 'YYYY-MM-DD')
$$;

CREATE OR REPLACE FUNCTION common.format_timestamp(dt_str STRING)
RETURNS TIMESTAMP_TZ
LANGUAGE SQL
AS
$$
    TO_TIMESTAMP_TZ(dt_str, 'YYYY-MM-DD HH24:MI:SS')
$$;

CREATE OR REPLACE FUNCTION common.format_timestamp_v2(dt_str STRING)
RETURNS TIMESTAMP_TZ
LANGUAGE SQL
AS
$$
   TRY_TO_TIMESTAMP_TZ(dt_str, 'YYYY-MM-DD"T"HH24:MI:SS')
$$;

CREATE OR REPLACE FUNCTION common.format_timestamp_with_f6(dt_str STRING)
RETURNS TIMESTAMP_TZ
LANGUAGE SQL
AS
$$
    try_to_timestamp_tz(dt_str, 'YYYY-MM-DD HH24:MI:SS.FF6')
$$;

CREATE OR REPLACE FUNCTION common.format_timestamp_with_f9(dt_str STRING)
RETURNS TIMESTAMP_TZ
LANGUAGE SQL
AS
$$
    try_to_timestamp_tz(dt_str, 'YYYY-MM-DD HH24:MI:SS.FF9')
$$;

