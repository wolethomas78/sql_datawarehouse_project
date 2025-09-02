
# ETL Pipeline Project with Medallion Architecture (PostgreSQL)

## Project Overview
This project implements a Medallion Architecture ETL pipeline using PostgreSQL. It ingests raw CRM and ERP CSV datasets (Bronze layer), performs data cleaning and transformations (Silver layer), and creates analytics-ready views (Gold layer). The pipeline leverages **stored procedures**, **logging**, and tracks **row counts and ETL performance**.
  
- **Source Systems:** CRM and ERP datasets in CSV format  
- **Bronze Layer:** Ingest raw CSV files into PostgreSQL using stored procedures  
- **Silver Layer:** Clean and standardize data, with **logging** to track:  
  - Runtime of ETL jobs  
  - Row counts at each stage  
  - Errors and anomalies  
- **Gold Layer:** Create **business-ready views** and model a **star schema** for analytics  

The solution highlights:
- **End-to-end ETL design** (from raw files to analytics views)  
- **Stored procedure orchestration** in PostgreSQL  
- **Operational monitoring** (logging, error handling, performance tracking)  
- **Data warehouse best practices** with layered architecture  

---

## Architecture & Diagrams

### 1. Data Warehouse Architecture
![DWH Architecture](https://github.com/wolethomas78/sql_datawarehouse_project/blob/6ee175a66102e0bc575fbe67f3a3332d318448a7/DWH_Architecture.png)

### 2. Medallion Architecture Layers
![Architecture Layers](https://github.com/wolethomas78/sql_datawarehouse_project/blob/6ee175a66102e0bc575fbe67f3a3332d318448a7/DWH_Layer.png)

### 3. Data Flow
![Data Flow](https://github.com/wolethomas78/sql_datawarehouse_project/blob/6ee175a66102e0bc575fbe67f3a3332d318448a7/Data_flow.png)

### 4. Data Integration (CRM + ERP)
![Data Integration](https://github.com/wolethomas78/sql_datawarehouse_project/blob/6ee175a66102e0bc575fbe67f3a3332d318448a7/data_integration.png)

### 5. Star Schema (Gold Layer)
![Star Schema](https://github.com/wolethomas78/sql_datawarehouse_project/blob/6ee175a66102e0bc575fbe67f3a3332d318448a7/star%20schema.png)

### 6. Data Warehouse Layers
![DWH Layer]()

---

## 📂 Datasets

### CRM Folder
- `CUST_AZ12.csv` → Customer IDs and metadata  
- `cust_info.csv` → Customer personal details  
- `LOC_a101.csv` → Customer location info  

### ERP Folder
- `prd_info.csv` → Product catalog  
- `px_cat_g1v2.csv` → Product category & pricing  
- `sales_details.csv` → Sales transactions  

---

## ⚙️ ETL Pipeline Steps

### 🔹 Bronze Layer
- **CSV ingestion:** Copied 6 CSVs (CRM + ERP) into PostgreSQL staging tables.  
- **Stored procedures:** Automated the load process.  
- **Logging:**  
  - Recorded job runtime for each table load  
  - Counted number of rows loaded  
  - Tracked and reported any load errors  

### 🔹 Silver Layer
- **Data cleaning & standardization:**  
  - Removed duplicates  
  - Handled missing values  
  - Enforced correct data types  
- **Stored procedures:** Used to orchestrate cleaning steps.  
- **Logging:**  
  - Row counts before & after cleaning  
  - Error capture (invalid formats, null violations, duplicates)  
  - Transformation runtime  

### 🔹 Gold Layer
- **Views creation:** Built analytics-ready views for sales, products, and customers.  
- **Star schema design:** Modeled a fact table (sales) with dimension tables (customer, product, location).  

---

## 🔎 Example Queries
```sql
-- Top 10 customers by total sales
SELECT c.customer_name, SUM(s.amount) AS total_sales
FROM gold_sales_view s
JOIN gold_customers_view c ON s.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_sales DESC
LIMIT 10;

-- Total sales by product category
SELECT p.category_name, SUM(s.amount) AS total_sales
FROM gold_sales_view s
JOIN gold_product_view p ON s.product_id = p.product_id
GROUP BY p.category_name
ORDER BY total_sales DESC;


