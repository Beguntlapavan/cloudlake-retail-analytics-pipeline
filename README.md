# CloudLake Retail Analytics Pipeline

## Overview
This project is an end-to-end data engineering pipeline built using AWS S3 and Databricks.  
It processes raw retail sales data, performs transformations, and creates analytics-ready datasets using Delta Lake.

---

## Tech Stack
- AWS S3
- Databricks
- PySpark
- Delta Lake
- SQL
- Python

---

## Architecture
S3 (Raw Data) → Databricks → Bronze → Silver → Gold → Analytics Output

---

## Project Workflow

1. Raw CSV file is uploaded to AWS S3
2. Databricks reads data from S3
3. Data is stored in Bronze layer (raw format)
4. Data is cleaned and transformed into Silver layer
5. Aggregations are created in Gold layer
6. Final data is used for reporting and analysis

---

## Dataset
Sample retail dataset with fields:

- order_id  
- customer_id  
- product  
- category  
- quantity  
- price  
- order_date  
- region  

---

## Key Features

- End-to-end data pipeline using Databricks
- Delta Lake implementation (ACID transactions)
- Bronze, Silver, Gold architecture
- PySpark transformations
- Basic data validation
- Scalable design using cloud storage

---

## Folder Structure
