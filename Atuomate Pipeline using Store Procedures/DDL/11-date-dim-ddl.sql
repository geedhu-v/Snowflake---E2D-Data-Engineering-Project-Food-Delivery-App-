use role sysadmin;
use warehouse adhoc_wh;
use database sp_pipeline_db;
use schema consumption_sch;

CREATE OR REPLACE TABLE CONSUMPTION_SCH.DATE_DIM (
    DATE_DIM_HK NUMBER PRIMARY KEY comment 'Date Dim HK (EDW)',   -- Surrogate key for date dimension
    CALENDAR_DATE DATE UNIQUE,                     -- The actual calendar date
    YEAR NUMBER,                                   -- Year
    QUARTER NUMBER,                                -- Quarter (1-4)
    MONTH NUMBER,                                  -- Month (1-12)
    WEEK NUMBER,                                   -- Week of the year
    DAY_OF_YEAR NUMBER,                            -- Day of the year (1-365/366)
    DAY_OF_WEEK NUMBER,                            -- Day of the week (1-7)
    DAY_OF_THE_MONTH NUMBER,                       -- Day of the month (1-31)
    DAY_NAME STRING                                -- Name of the day (e.g., Monday)
)
comment = 'Date dimension table created using min of order data.';
