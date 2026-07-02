import os
from pyspark.sql import SparkSession
from my_project.tasks.bronze.bronze_online_tcg import run_bronze
from my_project.tasks.bronze.bronze_salesforce import run_bronze_salesforce
from my_project.tasks.silver.silver_task import run_silver
from my_project.tasks.gold.dim_customers import run_dim_customers
from my_project.tasks.gold.fact_orders import run_fact_orders
from my_project.tasks.gold.dim_leads import run_dim_leads
from my_project.utils.pipeline_config_loader import load_pipeline_config, get_path

from my_project.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

PIPELINE_CONFIG_PATH = "config/pipeline_config.yaml"


def run_pipeline(spark: SparkSession, config: dict) -> None:
    """
    Runs the full medallion pipeline end to end.
    All paths driven by pipeline_config.yaml.

    Bronze → Silver → Gold

    In Databricks each of these would be a separate job
    with paths passed as job parameters.
    """

    # Extract paths from config
    raw_path = get_path(config, "paths", "raw")
    bronze_path = get_path(config, "paths", "bronze")
    silver_config_path = get_path(config, "silver", "config_path")
    silver_path = get_path(config, "paths", "silver")
    gold_path = get_path(config, "paths", "gold")

    # Extract source file names
    customers_file = get_path(config, "sources", "online_tcg", "customers")
    orders_file = get_path(config, "sources", "online_tcg", "orders")
    leads_file = get_path(config, "sources", "salesforce", "leads")

    # ===========================
    # BRONZE
    # ===========================
    logger.info("=" * 50)
    logger.info("STARTING BRONZE LAYER")
    logger.info("=" * 50)

    run_bronze(
        spark,
        customers_input=f"{raw_path}/{customers_file}",
        orders_input=f"{raw_path}/{orders_file}",
        output_path=bronze_path,
    )

    run_bronze_salesforce(
        spark, leads_input=f"{raw_path}/{leads_file}", output_path=bronze_path
    )

    # ===========================
    # SILVER
    # ===========================
    logger.info("=" * 50)
    logger.info("STARTING SILVER LAYER")
    logger.info("=" * 50)

    run_silver(spark, config_path=silver_config_path)

    # ===========================
    # GOLD
    # ===========================
    logger.info("=" * 50)
    logger.info("STARTING GOLD LAYER")
    logger.info("=" * 50)

    run_dim_customers(
        spark,
        input_path=f"{silver_path}/customers",
        output_path=f"{gold_path}/dim_customers",
    )

    run_fact_orders(
        spark,
        orders_input_path=f"{silver_path}/orders",
        dim_customers_path=f"{gold_path}/dim_customers",
        output_path=f"{gold_path}/fact_orders",
    )

    run_dim_leads(
        spark, input_path=f"{silver_path}/leads", output_path=f"{gold_path}/dim_leads"
    )

    logger.info("=" * 50)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 50)


def show_results(spark: SparkSession, config: dict) -> None:
    """
    Prints the output of each layer to the console.
    Includes quarantine output so data quality issues are visible.
    """
    bronze_path = get_path(config, "paths", "bronze")
    silver_path = get_path(config, "paths", "silver")
    gold_path = get_path(config, "paths", "gold")
    quarantine_path = get_path(config, "paths", "quarantine")

    # ===========================
    # BRONZE
    # ===========================
    print("\n" + "=" * 50)
    print("BRONZE: Customers (raw)")
    print("=" * 50)
    spark.read.parquet(f"{bronze_path}/customers").show(truncate=False)

    print("\n" + "=" * 50)
    print("BRONZE: Orders (raw)")
    print("=" * 50)
    spark.read.parquet(f"{bronze_path}/orders").show(truncate=False)

    print("\n" + "=" * 50)
    print("BRONZE: Salesforce Leads (raw)")
    print("=" * 50)
    spark.read.parquet(f"{bronze_path}/leads").show(truncate=False)

    # ===========================
    # SILVER
    # ===========================
    print("\n" + "=" * 50)
    print("SILVER: Customers (cleaned + SCD2)")
    print("=" * 50)
    spark.read.parquet(f"{silver_path}/customers").show(truncate=False)

    print("\n" + "=" * 50)
    print("SILVER: Orders (cleaned)")
    print("=" * 50)
    spark.read.parquet(f"{silver_path}/orders").show(truncate=False)

    print("\n" + "=" * 50)
    print("SILVER: Leads (cleaned + SCD2)")
    print("=" * 50)
    spark.read.parquet(f"{silver_path}/leads").show(truncate=False)

    # ===========================
    # QUARANTINE
    # ===========================
    print("\n" + "=" * 50)
    print("QUARANTINE: Customers (missing date columns)")
    print("=" * 50)
    customers_quarantine = f"{quarantine_path}/customers"
    if (
        os.path.exists(customers_quarantine)
        and len(os.listdir(customers_quarantine)) > 0
    ):
        spark.read.parquet(customers_quarantine).show(truncate=False)
    else:
        print("No quarantined customer records")

    print("\n" + "=" * 50)
    print("QUARANTINE: Leads (missing date columns)")
    print("=" * 50)
    leads_quarantine = f"{quarantine_path}/leads"
    if os.path.exists(leads_quarantine) and len(os.listdir(leads_quarantine)) > 0:
        spark.read.parquet(leads_quarantine).show(truncate=False)
    else:
        print("No quarantined lead records")

    # ===========================
    # GOLD
    # ===========================
    print("\n" + "=" * 50)
    print("GOLD: dim_customers")
    print("=" * 50)
    spark.read.parquet(f"{gold_path}/dim_customers").show(truncate=False)

    print("\n" + "=" * 50)
    print("GOLD: fact_orders")
    print("=" * 50)
    spark.read.parquet(f"{gold_path}/fact_orders").show(truncate=False)

    print("\n" + "=" * 50)
    print("GOLD: dim_leads")
    print("=" * 50)
    spark.read.parquet(f"{gold_path}/dim_leads").show(truncate=False)


def show_quarantine_summary(spark: SparkSession, config: dict) -> None:
    """
    Prints a summary of quarantined records.
    """
    quarantine_path = get_path(config, "paths", "quarantine")

    print("\n" + "=" * 50)
    print("QUARANTINE SUMMARY")
    print("=" * 50)

    total_quarantined = 0

    for table in ["customers", "leads"]:
        path = f"{quarantine_path}/{table}"
        if os.path.exists(path) and len(os.listdir(path)) > 0:
            count = spark.read.parquet(path).count()
            total_quarantined += count
            print(f"  {table}: {count} quarantined records")
        else:
            print(f"  {table}: 0 quarantined records")

    print(f"\n  Total quarantined: {total_quarantined}")
    print("=" * 50)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("medallion_pipeline")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    config = load_pipeline_config(PIPELINE_CONFIG_PATH)

    run_pipeline(spark, config)
    show_results(spark, config)
    show_quarantine_summary(spark, config)
