README.md 

# Customer Analytics Big Data Pipeline 

 

## Overview 

This project implements an end-to-end big data analytics pipeline for customer behavior analysis in an e-commerce context.   

The pipeline ingests data from multiple sources, processes it using distributed computing, stores the results in an analytical database, and exposes insights through a business intelligence dashboard. 

 

The main goal is to demonstrate practical skills in ETL design,PySpark, DuckDB,workflow orchestration,and BI reporting. 

 

 Business Problem 

E-commerce platforms generate large volumes of customer data across orders, purchases, and reviews.   

This project answers questions such as: 

- Who are the highest-value customers? 

- How do customer ratings and sentiment relate to revenue? 

- How do orders, purchases, and reviews differ across customers? 

 

 **Data Source**

The pipeline integrates three distinct datasets: 

1. Customer Reviews (CSV)   

2. Orders Data (Parquet)  

3. Purchase History (Parquet)   

   

All datasets are related using customer_id. 

 

 Architecture 

Data Sources (CSV, Parquet,parquet) 
↓ 
Apache PySpark (ETL & Aggregation) 
↓ 
DuckDB (Analytical Storage) 
↓ 
Power BI Dashboard 

 

 Technologies Used 

- Apache PySpark – Distributed data processing 

- DuckDB – High-performance analytical database 

- Prefect – Pipeline orchestration & scheduling 

- Power BI – Interactive dashboard & visualization 

- Python– Core programming language 

 

Project Structure 

big-data-customer-analytics/ 
│ 
├── data Raw CSV & Parquet datasets 
├── pipeline 
│ ├── etl_pipeline.py Main PySpark ETL pipeline 
│ └── prefect_flow.py Prefect orchestration flow 
│ 
├── output 
│ ├── customer_analytics.duckdb 
│ └── customer_analytics.csv 
│ 
├── dashboard 
│ └── powerbi_screenshots 
│ 
├── README.md 
└── requirements.txt 

 

 ETL Process 

 1. Extract 

- Load CSV and Parquet files using PySpark 

- Infer schemas and validate row counts 

 

 2. Transform 

- Clean and cast data types 

- Aggregate metrics per customer: 

  - Total orders 

  - Total purchases 

  - Total revenue 

  - Average rating 

  - Sentiment score 

- Merge all datasets using customer_id 

- Handle missing values 

 

 3. Load 

- Store final aggregated dataset in DuckDB 

- Optional CSV backup for portability 

 

 

 Orchestration 

The pipeline is orchestrated using Prefect: 

- Automated daily execution 

- Monitored pipeline runs 

- Production-ready deployment 

 

Screenshots of Prefect flow runs and deployment are included in the repository. 

 

 

 Dashboard 

The final dataset is visualized in Power BI, showing: 

- Total revenue 

- Total customers 

- Average customer value 

- Top customers by revenue 

- Rating vs revenue analysis 

- Orders vs purchases comparison 

 How to Run the Project 

 

1 pip install pyspark duckdb pandas prefect 

2. Run ETL Pipeline 

python pipeline/etl_pipeline.py 

3. Run with Prefect 

prefect deploy 

prefect agent start 

 

Results 

Unified customer analytics table stored in DuckDB 

Scalable and repeatable ETL pipeline 

Business-ready insights available via Power BI



