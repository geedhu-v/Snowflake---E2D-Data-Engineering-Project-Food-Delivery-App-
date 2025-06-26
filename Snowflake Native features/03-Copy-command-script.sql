-- use sysadmin role.
use role sysadmin;

-- create a demo database 
create or replace database demo_db;
use database demo_db;
use schema public;

-- ---------------------------------------------
-- Next Step
-- ---------------------------------------------

-- Create table for Departments
CREATE TABLE demo_db.public.departments (
    department_id INT PRIMARY KEY, -- Unique identifier for each department
    department_name VARCHAR(100) NOT NULL, -- Name of the department
    create_date TIMESTAMP NOT NULL, -- Date when the department was created
    modified_date TIMESTAMP NOT NULL -- Last modified date of the department
);

-- Create table for Employees
CREATE TABLE demo_db.public.employees (
    emp_id INT PRIMARY KEY, -- Unique identifier for each employee
    name VARCHAR(100) NOT NULL, -- Name of the employee
    email VARCHAR(150) UNIQUE NOT NULL, -- Employee's email (must be unique)
    dob DATE NOT NULL, -- Date of birth
    mobile_number VARCHAR(20) NOT NULL, -- Employee's mobile number
    designation VARCHAR(100) NOT NULL, -- Employee's designation/job title
    department_id INT NOT NULL, -- Foreign key linking to department_id
    create_date TIMESTAMP NOT NULL, -- Date when the employee record was created
    modified_date TIMESTAMP NOT NULL, -- Last modified date of the employee record
    CONSTRAINT fk_department FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

-- ---------------------------------------------
-- Next Step
-- ---------------------------------------------

 -- create file format to process the CSV file
create file format if not exists demo_db.public.csv_ff
        type = 'csv' 
        compression = 'auto' 
        field_delimiter = ',' 
        record_delimiter = '\n' 
        skip_header = 1 
        field_optionally_enclosed_by = '\042' 
        null_if = ('\\N');

-- create snowflake internal stage
create stage demo_db.public.internal_csv_stg
    directory = ( enable = true )
    comment = 'this is the snowflake internal stage';

-- ---------------------------------------------
-- Next Step
-- ---------------------------------------------
-- Query department csv data using $ notation
SELECT 
    t.$1::INT AS department_id,
    t.$2::TEXT AS department_name,
    t.$3::TIMESTAMP AS create_date,
    t.$4::TIMESTAMP AS modified_date
FROM @demo_db.public.internal_csv_stg/department/departments.csv
(file_format => 'demo_db.public.csv_ff') t;


-- Query employee csv data using $ notation
SELECT 
    t.$1::INT AS emp_id,
    t.$2::TEXT AS name,
    t.$3::TEXT AS email,
    t.$4::DATE AS dob,
    t.$5::TEXT AS mobile_number,
    t.$6::TEXT AS designation,
    t.$7::INT AS department_id,
    t.$8::TIMESTAMP AS create_date,
    t.$9::TIMESTAMP AS modified_date
FROM @demo_db.public.internal_csv_stg/employee/employees.csv
(file_format => 'demo_db.public.csv_ff') t;


-- ---------------------------------------------
-- Next Step
-- ---------------------------------------------
-- run copy command
copy into demo_db.public.departments 
FROM @demo_db.public.internal_csv_stg/department/departments.csv
FILE_FORMAT = (FORMAT_NAME = 'demo.public.csv_ff');

-- copy command with select statement
copy into demo_db.public.departments 
from (
    select 
        t.$1::INT AS department_id,
        t.$2::TEXT AS department_name,
        t.$3::TIMESTAMP AS create_date,
        t.$4::TIMESTAMP AS modified_date
    FROM @demo_db.public.internal_csv_stg/department-2/departments.csv t
    )
file_format = (format_name = 'demo.public.csv_ff')
force = true
on_error = continue;

select *
from table(information_schema.copy_history(table_name=>'DEPARTMENTS', start_time=> dateadd(hours, -1, current_timestamp())));
