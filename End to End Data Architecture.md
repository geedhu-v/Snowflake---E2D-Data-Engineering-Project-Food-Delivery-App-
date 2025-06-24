# End to End - Data Flow Architecture
<img width="923" alt="image" src="https://github.com/user-attachments/assets/53063111-98d3-4004-926c-6fde26956b3e" />

1. Data which is in CSV format is loaded into SNowflake using File loader, which is placed in Stage layer.
2. Copy command is used to load the data from Stage to a table.
3. Clean Layer: Now the table is ready to undergo some processing, cleaning and Transformation.
4. Consumption Layer: create fact and dimesnsion table
5. Dashboard: Some useful insights are visualized using Power BI.

### ER Diagram 
<img width="814" alt="image" src="https://github.com/user-attachments/assets/9d1b7a18-6e6e-43b9-9859-dea6562a1660" />

## Snowflake supports Structured, Semi-structured, and Unstructured data.

1. Structured Data: CSV, TSV
2. Semi-Structured: JSON, AVro, ORC, Parquet, and XML
3. Unstructured data can also be loaded, but it might require special handling or conversion before loading.

## Dimensional Modeling
1. It is often referred to as the Star Schema in the data warehousing domain, adheres to the principals of Second Normal Form (2NF) in database design.
2. A star schema typically consists of a central fact table surrounded by multiple dimension tables.
**Note: Fact tables derive from transactional data, while dimension tables originate from master data.**
3. Copy command transfers files from a stage location into SNowflake.
   
