
# ETL Pipeline Project with Medallion Architecture (PostgreSQL)

## Project Overview
This project implements a Medallion Architecture ETL pipeline using PostgreSQL. It ingests raw CRM and ERP CSV datasets (Bronze layer), performs data cleaning and transformations (Silver layer), and creates analytics-ready views (Gold layer). The pipeline leverages **stored procedures**, **logging**, and tracks **row counts and ETL performance**.

---

## Architecture Diagram
The pipeline follows the **Bronze → Silver → Gold** structure:
---
Bronze Layer (Raw CSVs)
│
▼
Silver Layer (Cleaned & Transformed Data)
│
▼
Gold Layer (Analytics-ready Views)
---

- **Bronze Layer:** Ingest raw CSV files into staging tables.
- **Silver Layer:** Data cleaning, deduplication, type casting, and logging.
- **Gold Layer:** Create views for analytics and reporting.

*(You can replace this ASCII diagram with a visual image in `docs/architecture_diagram.png`.)*

---

## Datasets

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

All files are located under `data/bronze/CRM` and `data/bronze/ERP`.

---

## ETL Pipeline Steps

1. **Bronze Layer**
   - Load CSV files into PostgreSQL staging tables.
   - Track errors and load time via logging stored procedures.

2. **Silver Layer**
   - Clean and standardize the data:
     - Handle missing values and duplicates
     - Convert data types as needed
     - Perform initial transformations for analysis
   - Log row counts and runtime for each transformation.

3. **Gold Layer**
   - Create views for analytics:
     - Customer sales summary
     - Product performance and revenue
     - Aggregated metrics for reporting

---

## Key Features
- Implements **Medallion Architecture** (Bronze → Silver → Gold)
- ETL orchestration using **PostgreSQL stored procedures**
- Detailed **logging and monitoring** for row counts, errors, and runtime
- Data cleaning, deduplication, and type casting
- Analytics-ready views for business insights

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














``` etl_pipeline_project/
│
├── README.md
├── data/
│   ├── bronze/
│   │   ├── CRM/
│   │   │   ├── CUST_AZ12.csv
│   │   │   ├── cust_info.csv
│   │   │   └── LOC_a101.csv
│   │   └── ERP/
│   │       ├── prd_info.csv
│   │       ├── px_cat_g1v2.csv
│   │       └── sales_details.csv
│   ├── silver/          # Optional: cleaned/intermediate tables
│   └── gold/            # Optional: final views or aggregated tables
│
├── sql/
│   ├── bronze/
│   │   └── load_bronze.sql           # Ingest CSVs into staging tables
│   ├── silver/
│   │   └── transform_silver.sql      # Cleaning & transformations
│   ├── gold/
│   │   └── create_gold_views.sql     # Final views/aggregates
│   ├── procedures/
│   │   └── etl_procedures.sql        # ETL orchestration stored procedures
│   └── logging/
│       └── etl_logging.sql           # Track errors, row counts, and load time
│
├── docs/
│   └── architecture_diagram.png      # Medallion pipeline visual
│
└── examples/ ```
    └── sample_queries.sql            # Example queries on Gold layer

