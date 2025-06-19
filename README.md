# Snowflake---E2D-Data-Engineering-Project-Food-Delivery-App-

## Project Objective:
**1. Data Flow Architecture:**
1. Design an efficient, scalable data flow architecture.

   
2. Implement Snowflake-specific features like COPY commands, streams, and merge statements.

   
**2. Data Modeling:**
Create fact and dimension tables, including SCD Type 2 for historical tracking.


**3. Handling Large Data Sets:**
Load and process large datasets efficiently, demonstrating Snowflake’s performance capabilities.


**4.Pipeline Automation:**
Automate the entire pipeline using stored procedures, tasks, and task trees for seamless workflows.


**5. Data Ingestion with GitHub Actions:**
To ingest data dynamically using GitHub Actions for real-time updates.

## Overall Data Flow Architecture 
![image](https://github.com/user-attachments/assets/ff96918f-3ac7-4aa7-b5b8-3e3abe371375)

## Entities associated with Food delivery App
<img width="469" alt="image" src="https://github.com/user-attachments/assets/8c516684-ea71-449c-b528-5a9792a726bb" />

## Objective: Business wants to determine the following metrics:
1. Total Revenue
2. Average Revenue per Order
3. Average Revenue per Item
4. Top Performing Restaurants
5. Revenue trends over time
6. Revenue by customer segment
7. Revenue by Restaurant and Location
8. Deliver Performance
9. Geographic Revenue Insights

## Problem Statement and Analysis
### Have you noted offers displaying on our mobile screens, tempting us to place orders before the deals vanish?
We all know that these deals are not created randomly. Food Platform like Uber Eats/Skip etc uses a data-driven approach to design these offers and customize them based on their user preferences.
**Goal is to retain the users and encourage them to spend more and more on the platform, increasing their overall revenue per customer per order.**

### Need for a powerful Data and Analytics Platform:
Consider a Food delivery app, serves 1.4 million food orders daily. Its presence is over 500 cities and towns, 140,000 restaurants on its platform, and 2.1 lakh delivery partners. In that case,
**It should have a powerful data and analytics platform to support their data-driven decision making.**

### Overview
1. Millions of users place orders, and these transactions are stored in OLTP Systems (SQL Server, PostgreSQL, MySQL, Oracle) and some in SAS.
2. The transactional data that is captured by the OLTP system is then moved to their data warehouse or data lake platform for reporting, advanced analytics and machine learning.
3. To handle such workloads with high volume and high speed, massive parallel processing, MPP or distributed computing like Apache Spark or Databricks or clous service like Azure, AWS, GCP is required.
   However, Snowflake is used to build a powerful self-service warehouse system where we don't have to worry about any infrastructure setup.
   
<img width="785" alt="image" src="https://github.com/user-attachments/assets/77593b69-995c-4528-924a-9907ae270ccc" />   

## Process considered
A food delivery app has many processes, here we consider only **"Food Order Booking and Receiving Flow"**
<img width="586" alt="image" src="https://github.com/user-attachments/assets/0d5cf6a7-f890-4283-94b7-7c353b6781ac" />

### Data Modeling
Data Modeling is a way of taking business requirements and information and structuring, organizing that information in the form of tables, so that it can be queried to find out answers to several business questions.
It involves three stages:

                        1. Conceptual: Highly abstract, identify the business process and ask clarifying questions.
                        
                        2. Logical - Identify Entities and attributes
                        
                        3. Physical - Identify tables and Columns associated 

### Conceptual Modeling: Entities associated with the Process: "Food Order Booking and Receiving Flow"
<img width="455" alt="image" src="https://github.com/user-attachments/assets/e6eca2e7-ae0c-4590-a655-c09997ed451e" />









