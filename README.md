etl_pipeline_project/
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
└── examples/
    └── sample_queries.sql            # Example queries on Gold layer

