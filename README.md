
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
- **Logging:**  ![Tracker Output](https://github.com/wolethomas78/sql_datawarehouse_project/blob/1fa4c253659d8ee443968e264d6b022deb93e24d/bronze_call_procedure_output)
  - Recorded job runtime for each table load  
  - Counted number of rows loaded  
  - Tracked and reported any load errors
    
```
  NOTICE:  Load Time: 118.922000 ms
NOTICE:  total no of rows in bronze_crm_cust_info: 18494
NOTICE:  Load Time: 8.672000 ms
NOTICE:  total no of rows in bronze_crm_prd_info: 397
NOTICE:  Load Time: 166.740000 ms
NOTICE:  total no of rows in bronze_crm_sales_details: 60398
NOTICE:  Load Time: 47.028000 ms
NOTICE:  total no of rows in bronze_erp_loc_a101: 18484
NOTICE:  Load Time: 119.562000 ms
NOTICE:  total no of rows in bronze_erp_cust_az12: 18484
NOTICE:  Load Time: 4.418000 ms
NOTICE:  total no of rows in bronze_erp_cat_g1v2: 37
CALL

Query returned successfully in 334 msec.
Total rows: 4 of 4
Query complete 00:00:00.334
Ln 224, Col
```
 ---

  - ![Bronze Layer Code](https://github.com/wolethomas78/sql_datawarehouse_project/blob/dd615ded764c169be1758d690023ed24493c7808/bronze_layer_code)
 ## Sample Bronze Layer Code:

```
	- Creating store procedure for re-useability
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
	END;
```
---

# Silver Layer
- **Data cleaning & standardization:**  
  - Removed duplicates  
  - Handled missing values  
  - Enforced correct data types  
- **Stored procedures:** Encapsulate the ETL load process (truncate, transform, insert, and log steps) and enable automation when scheduled through orchestration tools.  
- **Logging:**  
  - Row counts before & after cleaning  
  - Error capture (invalid formats, null violations, duplicates)  
  - Transformation runtime
 - ![Silver Layer code](https://github.com/wolethomas78/sql_datawarehouse_project/blob/62319e8a4fcbc7512ece4297ce03f59d513e8446/silver_layer_code)

## Sample Silver Layer Code

```
	CREATE OR REPLACE PROCEDURE silver_load()
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
```
---
### Gold Layer
- **Views creation:** Built analytics-ready views for sales, products, and customers.  
- **Star schema design:** Modeled a fact table (sales) with dimension tables (customer, product, location).```
- ![Gold Layer Fact Table Code](https://github.com/wolethomas78/sql_datawarehouse_project/blob/01faa555270c51619c956c660548d4bb803095cf/goald_fact_table)
- ![Gold Layer Product Code](https://github.com/wolethomas78/sql_datawarehouse_project/blob/01faa555270c51619c956c660548d4bb803095cf/gold_product_view)
- ![Gold Layer Customer Code](https://github.com/wolethomas78/sql_datawarehouse_project/blob/01faa555270c51619c956c660548d4bb803095cf/gold_dimension_code)

  ## Sample Gold Layer Code

```
	CREATE VIEW gold_load_dimension AS
--- Selecting customer info with enrichment from ERP location and customer tables
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,-- generating surrogate key
    ci.cst_id,                                -- Customer ID (from CRM table)
    ci.cst_key AS customer_number,               -- Customer key, renamed as customer_number
    ci.cst_firstname AS fisrt_name,           -- First name (typo: "fisrt_name" should be "first_name")
    ci.cst_lastname AS last_name,             -- Last name
    ci.cst_marital_status AS marital_status,  -- Marital status
    -- Derived gender field:
    CASE 
        WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr      -- If gender is not "n/a", keep CRM value
        ELSE COALESCE(ca.gen, 'n/a')                    -- Otherwise, fallback to ERP gender; if null, use "n/a"
    END AS gender,
    
    ca.bdate AS birthdate,        -- Birthdate from ERP customer table
    lo.cntry AS country,          -- Country from ERP location table
    ci.cst_create_date AS create_date -- Customer creation date from CRM
FROM silver_crm_cust_info ci
-- Join to ERP location table based on customer key
LEFT JOIN silver_erp_loc_a101 lo
    ON ci.cst_key = lo.cid
-- Join to ERP customer table based on customer key
LEFT JOIN silver_erp_cust_az12 ca
    ON ci.cst_key = ca.cid;


SELECT * FROM gold_load_dimension;
```

