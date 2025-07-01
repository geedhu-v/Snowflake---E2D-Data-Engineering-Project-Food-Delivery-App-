## Snowflake's Internal and External stage location offers 5 additional built-in columns that can be utilized for auditing purpose.

1. METADATA$FILENAME
2. METADATA$FILE_ROW_NUMBER
3. METADATA$FILE_CONTENT_KEY
4. METADATA$FILE_LAST_MODIFIED
5. METADATA$START_SCAN_TIME


## 🔍 Primary Key vs Surrogate Key

| Feature           | Primary Key                                 | Surrogate Key                          |
|------------------|----------------------------------------------|----------------------------------------|
| Definition        | Uniquely identifies a row using real data    | System-generated unique identifier     |
| Business Meaning  | Yes (e.g., email, SSN)                       | No (e.g., ID = 101, 102...)            |
| Changeable        | Might change if business data changes        | Never changes once assigned            |
| Readability       | Human-readable                               | Not meaningful to users                |
| Performance       | Can be slower if large or composite          | Typically faster due to simplicity     |
| Example           | email = "geedhu@example.com"                | user_id = 1001                         |


In short, every table needs a primary key, but a surrogate key can be used as the primary key when natural data isn’t reliable or stable enough.

## What will happen if the data from the stream object is not consumed using a merge statement or stored procedure
It will keep the bad records, and the merge statement will keep failing.

Solution: 

Step 1: To get rid of the bad record,  create a temporary table from the stream object as shown below:

<img width="263" alt="image" src="https://github.com/user-attachments/assets/f181e2fe-dd91-4402-9246-b4a8e8245e5e" />

Step 2: Delete the data from the stage_sch.location table

<img width="382" alt="image" src="https://github.com/user-attachments/assets/b6e9fa6b-0e11-498a-ae33-76c3dbab575e" />

Step 3: Now run the stream table, as it was created with "append-only" mode, it will not contain the deleted data history.

<img width="241" alt="image" src="https://github.com/user-attachments/assets/b1598021-cd72-4e8f-842b-12bac64b2bdd" />

<img width="914" alt="image" src="https://github.com/user-attachments/assets/8187284d-39bd-4ec9-835a-5a9b602e837b" />

While orchestrating an end-to-end pipeline, we should ensure that the merge statement never fails, using either of the two ways:
    1. In the stage schema, instead of having text, have the appropriate data type.
    2. While executing mer_statement, Use on error = "continue" instead of "abort_statement"

Modification needed to run the Copy Command that skips the bad records and does not corrupt Table Data.

Before making changes:
<img width="917" alt="image" src="https://github.com/user-attachments/assets/0cb8deeb-9c22-4281-af66-630dfe8c0050" />

After making changes while loading the corrupted data:
<img width="454" alt="image" src="https://github.com/user-attachments/assets/1c3b922b-1323-4b62-a7e8-11af9da7510e" />


    



