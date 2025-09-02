
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

### 6. Data Warehouse Layers Process
![DWH Layer Process](https://github.com/wolethomas78/sql_datawarehouse_project/blob/8f0ad247e1785eae46497cabed6fb1684f9381c4/BronzeLayer_Process.png)

---

## Datasets

### CRM Folder
- `CUST_AZ12.csv` → Customer IDs and metadata  
- `cust_info.csv` → Customer personal details  
- `LOC_a101.csv` → Customer location info  

### ERP Folder
- `prd_info.csv` → Product catalog  
- `px_cat_g1v2.csv` → Product category & pricing  
- `sales_details.csv` → Sales transactions  

---

## ETL Pipeline Steps

### 🔹 Bronze Layer
- **CSV ingestion:** Copied 6 CSVs (CRM + ERP) into PostgreSQL staging tables.  
- **Stored procedures:** For logic (loading, transforming, truncating, inserting, logging, etc.) into reusable blocks of SQL code. 
- **Logging:**  
  - Recorded job runtime for each table load  
  - Counted number of rows loaded  
  - Tracked and reported any load errors
  - ![Bronze Layer Code](https://github.com/wolethomas78/sql_datawarehouse_project/blob/dd615ded764c169be1758d690023ed24493c7808/bronze_layer_code)
 --- 
``` Creating store procedure for re-useability
CREATE OR REPLACE PROCEDURE bronze_load ()
language plpgsql
AS $$
-- Determing time taking to copy each file, display error if any and count rows in each file.
DECLARE
	start_time  TIMESTAMP; -- time when the upload starts
	end_time  TIMESTAMP; -- time when the upload ends
	duration  INTERVAL; -- difference btween end_time and start_time
	row_count  BIGINT; -- count the total rows in each file

BEGIN 
	

	BEGIN
			start_time := clock_timestamp();
		-- Bulk loading of the CSV file from the source
		-- Truncate and copy csv file into table bronze_crm_cust_info
		TRUNCATE TABLE bronze_crm_cust_info; 
		COPY bronze_crm_cust_info
		FROM 'C:\Program Files\PostgreSQL\16\cust_info.csv'
		DELIMITER ','
		CSV HEADER;
			end_time := clock_timestamp();
			duration := end_time - start_time;
      
		RAISE NOTICE 'Load Time: % ms', -- display the loading time in millisecond
			EXTRACT(MILLISECOND FROM duration) + EXTRACT(SECOND FROM duration)* 1000;
			
		-- count the total rows from bronze_crm_cust_info
		SELECT COUNT(*) INTO row_count FROM bronze_crm_cust_info;
		RAISE NOTICE 'total no of rows in bronze_crm_cust_info: %', row_count;
			
	EXCEPTION -- display error message if any error
		WHEN OTHERS THEN
		RAISE NOTICE 'no of errors during upload: %', SQLERRM;
	END;```

---

    ![Silver layer code](https://github.com/wolethomas78/sql_datawarehouse_project/blob/62319e8a4fcbc7512ece4297ce03f59d513e8446/silver_layer_code)
```# Silver Layer
- **Data cleaning & standardization:**  
  - Removed duplicates  
  - Handled missing values  
  - Enforced correct data types  
- **Stored procedures:** Encapsulate the ETL load process (truncate, transform, insert, and log steps) and enable automation when scheduled through orchestration tools.  
- **Logging:**  
  - Row counts before & after cleaning  
  - Error capture (invalid formats, null violations, duplicates)  
  - Transformation runtime
 ![Silver Layer code](https://github.com/wolethomas78/sql_datawarehouse_project/blob/62319e8a4fcbc7512ece4297ce03f59d513e8446/silver_layer_code)
```CREATE OR REPLACE PROCEDURE silver_load()
LANGUAGE plpgsql
AS $$
DECLARE 
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	duration INTERVAL;
	row_count BIGINT;
BEGIN

	BEGIN 
		start_time := clock_timestamp();
-- Truncate and copy csv file into table silver_crm_cust_info
TRUNCATE TABLE silver_crm_cust_info;
INSERT INTO silver_crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
) 

-- Select customer details with cleaned fields and latest record per customer
SELECT 
    cst_id,                                   -- Customer unique identifier
    cst_key,                                  -- Customer key 
    TRIM(cst_firstname) AS cst_firstname,     -- Trim spaces from first name
    TRIM(cst_lastname) AS cst_lastname,       -- Trim spaces from last name

    -- Map marital status codes to descriptive values
    CASE
        WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
        WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'n/a'                            -- Default for null/unknown values
    END AS cst_marital_status,

    -- Map gender codes to descriptive values
    CASE
        WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
        WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a'                            -- Default for null/unknown values
    END AS cst_gndr,	

    cst_create_date                           -- Record creation timestamp

FROM (
    -- Deduplicate customers by keeping only the latest record per cst_id
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC     -- Latest record first
        ) AS latest
    FROM bronze_crm_cust_info
    WHERE cst_id IS NOT NULL                  -- Exclude records without ID
### Gold Layer
- **Views creation:** Built analytics-ready views for sales, products, and customers.  
- **Star schema design:** Modeled a fact table (sales) with dimension tables (customer, product, location).```

---

## Example Queries
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


