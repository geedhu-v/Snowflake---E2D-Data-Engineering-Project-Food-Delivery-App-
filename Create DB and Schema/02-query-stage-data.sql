
-- change context (role/warehouse/schema)
use role sysadmin;

use warehouse adhoc_wh;
use schema sandbox.stage_sch;

-- show command
show stages;

-- list all the files inside stage location
list @stage_sch.csv_stg/initial/location/

-- SQL Command to check the data in stage location
-- process all the files inside the location
select 
    t.$1::text as locationid,
    t.$2::text as city,
    t.$3::text as state,
    t.$4::text as zipcode,
    t.$5::text as activeflag,
    t.$6::text as createddate,
    t.$7::text as modifieddate
from @stage_sch.csv_stg/initial/location/
(file_format => 'stage_sch.csv_file_format') t;

-- process specific file
select 
    t.$1::text as locationid,
    t.$2::text as city,
    t.$3::text as state,
    t.$4::text as zipcode,
    t.$5::text as activeflag,
    t.$6::text as createddate,
    t.$7::text as modifieddate
from @stage_sch.csv_stg/initial/location/location-5rows.csv 
(file_format => 'stage_sch.csv_file_format') t;