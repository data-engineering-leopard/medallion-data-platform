import logging
import os
from pyspark.sql import SparkSession
from my_project.tasks.bronze.bronze_online_tcg import run_bronze
from my_project.tasks.bronze.bronze_salesforce import run_bronze_salesforce
from my_project.tasks.silver.silver_task import run_silver
from my_project.tasks.gold.dim_customers import run_dim_customers
from my_project.tasks.gold.fact_orders import run_fact_orders
from my_project.tasks.gold.dim_leads import run_dim_leads

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# ===========================
# PATH CONSTANTS
# ===========================

RAW_PATH = "data/raw"
BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"
QUARANTINE_PATH = "quarantine/silver"


def run_pipeline(spark: SparkSession) -> None:
    """
    Runs the full medallion pipeline end to end.

    Bronze → Silver → Gold

    In Databricks each of these would be a separate job.
    Locally we run them in sequence for demonstration.
    """

    # ===========================
    # BRONZE
    # ===========================
    logger.info("=" * 50)
    logger.info("STARTING BRONZE LAYER")
    logger.info("=" * 50)

    run_bronze(
        spark,
        customers_input=f"{RAW_PATH}/customers.csv",
        orders_input=f"{RAW_PATH}/orders.csv",
        output_path=BRONZE_PATH
    )

    run_bronze_salesforce(
        spark,
        leads_input=f"{RAW_PATH}/sf_leads.csv",
        output_path=BRONZE_PATH
    )

    # ===========================
    # SILVER
    # ===========================
    logger.info("=" * 50)
    logger.info("STARTING SILVER LAYER")
    logger.info("=" * 50)

    run_silver(
        spark,
        config_path="config/schemas/silver"
    )

    # ===========================
    # GOLD
    # ===========================
    logger.info("=" * 50)
    logger.info("STARTING GOLD LAYER")
    logger.info("=" * 50)

    run_dim_customers(
        spark,
        input_path=f"{SILVER_PATH}/customers",
        output_path=f"{GOLD_PATH}/dim_customers"
    )

    run_fact_orders(
        spark,
        orders_input_path=f"{SILVER_PATH}/orders",
        dim_customers_path=f"{GOLD_PATH}/dim_customers",
        output_path=f"{GOLD_PATH}/fact_orders"
    )

    run_dim_leads(
        spark,
        input_path=f"{SILVER_PATH}/leads",
        output_path=f"{GOLD_PATH}/dim_leads"
    )

    logger.info("=" * 50)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 50)


def show_results(spark: SparkSession) -> None:
    """
    Prints the output of each layer to the console.
    Includes quarantine output so data quality issues are visible.
    """

    # ===========================
    # BRONZE
    # ===========================
    print("\n" + "=" * 50)
    print("BRONZE: Customers (raw)")
    print("=" * 50)
    spark.read.parquet(f"{BRONZE_PATH}/customers").show(truncate=False)

    print("\n" + "=" * 50)
    print("BRONZE: Orders (raw)")
    print("=" * 50)
    spark.read.parquet(f"{BRONZE_PATH}/orders").show(truncate=False)

    print("\n" + "=" * 50)
    print("BRONZE: Salesforce Leads (raw)")
    print("=" * 50)
    spark.read.parquet(f"{BRONZE_PATH}/leads").show(truncate=False)

    # ===========================
    # SILVER
    # ===========================
    print("\n" + "=" * 50)
    print("SILVER: Customers (cleaned + SCD2)")
    print("=" * 50)
    spark.read.parquet(f"{SILVER_PATH}/customers").show(truncate=False)

    print("\n" + "=" * 50)
    print("SILVER: Orders (cleaned)")
    print("=" * 50)
    spark.read.parquet(f"{SILVER_PATH}/orders").show(truncate=False)

    print("\n" + "=" * 50)
    print("SILVER: Leads (cleaned + SCD2)")
    print("=" * 50)
    spark.read.parquet(f"{SILVER_PATH}/leads").show(truncate=False)

    # ===========================
    # QUARANTINE
    # ===========================
    print("\n" + "=" * 50)
    print("QUARANTINE: Customers (missing date columns)")
    print("=" * 50)
    customers_quarantine = f"{QUARANTINE_PATH}/customers"
    if os.path.exists(customers_quarantine) and \
            len(os.listdir(customers_quarantine)) > 0:
        spark.read.parquet(customers_quarantine).show(truncate=False)
    else:
        print("No quarantined customer records")

    print("\n" + "=" * 50)
    print("QUARANTINE: Leads (missing date columns)")
    print("=" * 50)
    leads_quarantine = f"{QUARANTINE_PATH}/leads"
    if os.path.exists(leads_quarantine) and \
            len(os.listdir(leads_quarantine)) > 0:
        spark.read.parquet(leads_quarantine).show(truncate=False)
    else:
        print("No quarantined lead records")

    # ===========================
    # GOLD
    # ===========================
    print("\n" + "=" * 50)
    print("GOLD: dim_customers")
    print("=" * 50)
    spark.read.parquet(f"{GOLD_PATH}/dim_customers").show(truncate=False)

    print("\n" + "=" * 50)
    print("GOLD: fact_orders")
    print("=" * 50)
    spark.read.parquet(f"{GOLD_PATH}/fact_orders").show(truncate=False)

    print("\n" + "=" * 50)
    print("GOLD: dim_leads")
    print("=" * 50)
    spark.read.parquet(f"{GOLD_PATH}/dim_leads").show(truncate=False)


def show_quarantine_summary(spark: SparkSession) -> None:
    """
    Prints a summary of quarantined records.
    Useful for data quality monitoring.
    """
    print("\n" + "=" * 50)
    print("QUARANTINE SUMMARY")
    print("=" * 50)

    total_quarantined = 0

    for table in ["customers", "leads"]:
        path = f"{QUARANTINE_PATH}/{table}"
        if os.path.exists(path) and len(os.listdir(path)) > 0:
            count = spark.read.parquet(path).count()
            total_quarantined += count
            print(f"  {table}: {count} quarantined records")
        else:
            print(f"  {table}: 0 quarantined records")

    print(f"\n  Total quarantined: {total_quarantined}")
    print("=" * 50)


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("medallion_pipeline") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    run_pipeline(spark)
    show_results(spark)
    show_quarantine_summary(spark)