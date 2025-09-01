# ETL Pipeline Project with Medallion Architecture (PostgreSQL)

## Project Overview
This project implements a **Medallion Architecture ETL pipeline** using PostgreSQL.  
It ingests raw **CRM** and **ERP** CSV datasets (Bronze layer), performs **data cleaning and transformations** (Silver layer), and creates **analytics-ready views** (Gold layer).  

The pipeline leverages:
- **Stored procedures** for orchestration  
- **Logging** for errors, row counts, and runtime  
- **Star schema design** in the analytics layer  

---

## Architecture Diagram
![ETL Pipeline](https://github.com/wolethomas78/sql_datawarehouse_project/blob/905b331c27282d12cb310ae1e9d0b52a5c525b53/DWH_Architecture.png)

---

## Datasets
![Data Flow](https://github.com/wolethomas78/sql_datawarehouse_project/blob/7b1b0d99fc21e21df678a61f677b076b5d0bec28/Data_flow.png)

### CRM Folder
| File | Description |
|------|-------------|
| `CUST_AZ12.csv` | Raw customer IDs and metadata |
| `cust_info.csv` | Customer personal details |
| `LOC_a101.csv` | Customer location information |

### ERP Folder
| File | Description |
|------|-------------|
| `prd_info.csv` | Product catalog information |
| `px_cat_g1v2.csv` | Product category and pricing info |
| `sales_details.csv` | Sales transaction details |

---

## ETL Pipeline Steps

1. **Bronze Layer**
   - Load raw CSVs into PostgreSQL staging tables  
   - Log load times and errors  

2. **Silver Layer**
   - Data cleaning: remove duplicates, handle nulls, enforce data types  
   - Standardize CRM + ERP data for integration  
   - Log row counts and runtime  

3. **Gold Layer**
   - Build business-ready views for reporting  
   - Organize data into a **star schema**  

---

## Star Schema (Gold Layer)
![Star Schema](docs/star_schema.png)

---

## Key Features
- Implements **Medallion Architecture** (Bronze → Silver → Gold)  
- ETL orchestration using **PostgreSQL stored procedures**  
- **Error handling and performance logging**  
- Data cleaning & standardization in Silver layer  
- **Star schema design** in Gold layer for analytics  

---

## Example Queries (Gold Layer)
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


---

## Example Queries [Bronze layer](https://github.com/wolethomas78/sql_datawarehouse_project/blob/af8b612bb1e94eb932118f50bc7c1409850d950f/bronze_layer_code)
```-- Creating store procedure for re-useability
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






# [Silver layer](https://github.com/wolethomas78/sql_datawarehouse_project/blob/24b621863a96f6da11f1d3208541ae1053317658/silver_layer_code)

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
) y
WHERE latest = 1;                              -- Keep only the latest record per customer

		end_time := clock_timestamp();
		duration := end_time - start_time;
	RAISE NOTICE 'Load Time: % ms', -- display the loading time in millisecond
	EXTRACT(MILLISECOND FROM duration) + EXTRACT(SECOND FROM duration)* 1000;

	SELECT COUNT(*) INTO row_count FROM silver_crm_cust_info;
			RAISE NOTICE 'total no of rows in silver_crm_cust_info: %', row_count;
			
	EXCEPTION -- display error message if any error
		WHEN OTHERS THEN
		RAISE NOTICE 'no of errors during upload: %', SQLERRM;
	END;

















