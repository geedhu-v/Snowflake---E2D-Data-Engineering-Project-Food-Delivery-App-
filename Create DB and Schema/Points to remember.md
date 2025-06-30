## Snowflake's Internal and External stage location offers 5 additional built-in columns that can be utilized for auditing purpose.

1. METADATA$FILENAME
2. METADATA$FILE_ROW_NUMBER
3. METADATA$FILE_CONTENT_KEY
4. METADATA$FILE_LAST_MODIFIED
5. METADATA$START_SCAN_TIME


## 🔍 Primary Key vs Surrogate Key

| Feature | Primary Key | Surrogate Key | 
| Definition | Uniquely identifies a row using real data | System-generated unique identifier | 
| Business Meaning | Yes (e.g., email, SSN) | No (e.g., ID = 101, 102...) | 
| Changeable | Might change if business data changes | Never changes once assigned | 
| Readability | Human-readable | Not meaningful to users | 
| Performance | Can be slower if large or composite | Typically faster due to simplicity | 
| Example | email = "geedhu@example.com" | user_id = 1001 | 


In short, every table needs a primary key, but a surrogate key can be used as the primary key when natural data isn’t reliable or stable enough.
