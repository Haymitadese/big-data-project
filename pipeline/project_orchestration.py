import os
import shutil
import time
from datetime import timedelta
from prefect import task, flow
from prefect.artifacts import create_markdown_artifact
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import duckdb

# ==========================================
# 🔧 WINDOWS ENVIRONMENT FIX (CRITICAL)
# ==========================================
# Ensures Spark can write files on Windows without crashing
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['hadoop.home.dir'] = r"C:\hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";" + r"C:\hadoop\bin"
.
# ==========================================
# ⚙️ ADVANCED CONFIGURATION
# ==========================================
class ProjectConfig:
    PROJECT_NAME = "Customer Analysis Pipeline"
    BASE_DIR = r"C:\Users\HP 15\Desktop\big data project\data"
    
    # Inputs
    CSV_PATH = os.path.join(BASE_DIR, "nigerian_retail_and_ecommerce_customer_review_and_ratings_data.csv")
    ORDERS_PATH = os.path.join(BASE_DIR, "nigerian_retail_and_ecommerce_ecommerce_order_data.parquet")
    PURCHASE_PATH = os.path.join(BASE_DIR, "nigerian_retail_and_ecommerce_purchase_history_records.parquet")
    
    # Outputs
    TEMP_OUTPUT = r"C:\big_data_processed\temp_spark_output"
    DB_PATH = r"C:\big_data_processed\processed\customer_analytics.duckdb"
    
    # Settings
    SPARK_MEMORY = "8g"

# ==========================================
# 🛠️ ETL TASKS
# ==========================================

@task(name="Extract & Transform (Spark)", 
      retries=2, 
      retry_delay_seconds=10, 
      log_prints=True,
      description="Resilient Spark job with auto-retry logic.")
def spark_etl_process():
    print(f"🚀 Initializing Spark Session ({ProjectConfig.SPARK_MEMORY})...")
    spark = SparkSession.builder \
        .appName("CustomerAnalysis_ETL") \
        .config("spark.driver.memory", ProjectConfig.SPARK_MEMORY) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()

    # 1. LOAD
    print("📂 Ingesting Source Data...")
    df_reviews = spark.read.option("header", True).option("inferSchema", True).csv(ProjectConfig.CSV_PATH)
    df_orders = spark.read.parquet(ProjectConfig.ORDERS_PATH)
    df_purchase = spark.read.parquet(ProjectConfig.PURCHASE_PATH)

    # 2. TRANSFORM
    print("⚙️ Distributed Transformations...")
    
    # Reviews Aggregation
    df_reviews_agg = df_reviews.select("customer_id", "review_id", "rating", "sentiment_score") \
        .withColumn("rating", F.col("rating").cast("double")) \
        .withColumn("sentiment_score", F.col("sentiment_score").cast("double")) \
        .groupBy("customer_id").agg(
            F.count("review_id").alias("total_reviews"),
            F.avg("rating").alias("avg_rating"),
            F.avg("sentiment_score").alias("avg_sentiment_score")
        )

    # Orders Aggregation
    df_orders_agg = df_orders.select("customer_id", "order_id", "order_value_ngn", "shipping_fee_ngn") \
        .withColumn("order_value_ngn", F.col("order_value_ngn").cast("double")) \
        .withColumn("shipping_fee_ngn", F.col("shipping_fee_ngn").cast("double")) \
        .groupBy("customer_id").agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_value_ngn").alias("total_order_value"),
            F.sum("shipping_fee_ngn").alias("total_shipping_fee")
        )

    # Purchases Aggregation
    df_purchase_agg = df_purchase.select("customer_id", "total_amount_ngn", "quantity", "purchase_date") \
        .withColumn("total_amount_ngn", F.col("total_amount_ngn").cast("double")) \
        .withColumn("quantity", F.col("quantity").cast("int")) \
        .groupBy("customer_id").agg(
            F.count("purchase_date").alias("total_purchases"),
            F.sum("total_amount_ngn").alias("total_purchase_value"),
            F.sum("quantity").alias("total_quantity")
        )

    # 3. MERGE
    print("🔗 Merging DataFrames...")
    df_merged = df_reviews_agg.join(df_orders_agg, on="customer_id", how="full_outer") \
                              .join(df_purchase_agg, on="customer_id", how="full_outer") \
                              .fillna(0)

    # 4. WRITE
    if os.path.exists(ProjectConfig.TEMP_OUTPUT):
        shutil.rmtree(ProjectConfig.TEMP_OUTPUT, ignore_errors=True)

    print(f"💾 Writing Staging Data: {ProjectConfig.TEMP_OUTPUT}")
    df_merged.write.mode("overwrite").parquet(ProjectConfig.TEMP_OUTPUT)
    
    count = df_merged.count()
    print(f"✅ Spark Finished. Processed {count:,} records.")
    spark.stop()
    return ProjectConfig.TEMP_OUTPUT

@task(name="Load to DuckDB", log_prints=True)
def load_to_duckdb(parquet_folder_path: str):
    print(f"🦆 Loading into DuckDB: {ProjectConfig.DB_PATH}")
    os.makedirs(os.path.dirname(ProjectConfig.DB_PATH), exist_ok=True)
    con = duckdb.connect(ProjectConfig.DB_PATH)
    
    # Bulk Load
    con.execute(f"""
        CREATE OR REPLACE TABLE customer_analytics AS 
        SELECT * FROM read_parquet('{parquet_folder_path}/*.parquet');
    """)
    
    row_count = con.execute("SELECT COUNT(*) FROM customer_analytics").fetchone()[0]
    print(f"🎉 Load Complete. {row_count:,} rows ready.")
    con.close()
    return row_count

@task(name="Generate Business Report", log_prints=True, description="Generates a Markdown Artifact for the Dashboard.")
def generate_quality_report():
    print("🔍 Generating Advanced Data Report...")
    con = duckdb.connect(ProjectConfig.DB_PATH)
    
    # 1. Calculation: Real Business Metrics
    total_customers = con.execute("SELECT COUNT(*) FROM customer_analytics").fetchone()[0]
    total_revenue = con.execute("SELECT SUM(total_purchase_value) FROM customer_analytics").fetchone()[0]
    top_customer_val = con.execute("SELECT MAX(total_order_value) FROM customer_analytics").fetchone()[0]
    
    # Check for empty table (Quality Gate)
    if total_customers == 0:
        raise ValueError("❌ Critical Error: Database is empty after load!")

    # 2. Advanced Feature: Create UI Artifact
    # This creates a visible report in the Prefect Dashboard
    markdown_report = f"""
# 📊 Nigerian Retail Pipeline Report

## 🟢 Executive Summary
| Metric | Value |
|:-------|:------|
| **Total Customers** | `{total_customers:,}` |
| **Total Revenue** | `₦{total_revenue:,.2f}` |
| **Highest Single LTV** | `₦{top_customer_val:,.2f}` |

## ✅ System Health
- **Spark Job:** Success (Resilient Mode)
- **DuckDB Load:** Success (Bulk Insert)
- **Quality Check:** Passed
    """
    
    create_markdown_artifact(
        key="daily-retail-report",
        markdown=markdown_report,
        description="Daily Business Metrics"
    )
    
    print("✅ Artifact Generated! Check the 'Artifacts' tab in the Dashboard.")
    con.close()

# ==========================================
# 🌊 MAIN FLOW
# ==========================================
@flow(name=ProjectConfig.PROJECT_NAME, log_prints=True)
def main_flow():
    print(f"🚀 STARTING PIPELINE: {ProjectConfig.PROJECT_NAME}")
    
    # Step 1: Distributed Processing
    staging_path = spark_etl_process()
    
    # Step 2: Analytical Loading
    load_to_duckdb(staging_path)
    
    # Step 3: Reporting & Observability (Advanced)
    generate_quality_report()
    
    print("✅ End-to-End Pipeline Finished.")

if __name__ == "__main__":
    # ---------------------------------------------------------
    # PART 1: RUN ONCE (Visual Verification)
    # ---------------------------------------------------------
    main_flow()
    
    print("\n" + "█"*60)
    print("✨ ADVANCED FEATURE: ARTIFACTS REPORT GENERATED ✨")
    print("█"*60)
    print("1. Scroll UP to find: 'Starting temporary server on http://127.0.0.1:xxxxx'")
    print("   (OR check http://127.0.0.1:5000 if your local server is running)")
    print("2. Open the link.")
    print("3. Click the 'Artifacts' tab on the left.")
    print("4. Take a screenshot of the Business Table.")
    print("█"*60 + "\n")
    
    input("👉 Press ENTER to finish pipeline and register Schedule... ")

    # ---------------------------------------------------------
    # PART 2: PRODUCTION SCHEDULE (Bonus Requirements)
    # ---------------------------------------------------------
    print("\n📅 Registering Daily Schedule (09:00 AM)...")
    
    main_flow.serve(
        name="daily-customer-analysis-production",
        cron="0 9 * * *", 
        tags=["production", "spark", "duckdb"],
        description="Daily pipeline with automated Reporting Artifacts."
    )