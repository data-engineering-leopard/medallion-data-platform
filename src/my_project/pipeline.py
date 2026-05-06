import logging
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
        customers_input="data/raw/customers.csv",
        orders_input="data/raw/orders.csv",
        output_path="data/bronze"
    )

    run_bronze_salesforce(
        spark,
        leads_input="data/raw/sf_leads.csv",
        output_path="data/bronze"
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
        input_path="data/silver/customers",
        output_path="data/gold/dim_customers"
    )

    run_fact_orders(
        spark,
        orders_input_path="data/silver/orders",
        dim_customers_path="data/gold/dim_customers",
        output_path="data/gold/fact_orders"
    )

    run_dim_leads(
        spark,
        input_path="data/silver/leads",
        output_path="data/gold/dim_leads"
    )

    logger.info("=" * 50)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 50)


def show_results(spark: SparkSession) -> None:
    """
    Prints the output of each layer to the console.
    """
    print("\n" + "=" * 50)
    print("BRONZE: Customers (raw)")
    print("=" * 50)
    spark.read.parquet("data/bronze/customers").show(truncate=False)

    print("\n" + "=" * 50)
    print("BRONZE: Orders (raw)")
    print("=" * 50)
    spark.read.parquet("data/bronze/orders").show(truncate=False)

    print("\n" + "=" * 50)
    print("BRONZE: Salesforce Leads (raw)")
    print("=" * 50)
    spark.read.parquet("data/bronze/leads").show(truncate=False)

    print("\n" + "=" * 50)
    print("SILVER: Customers (cleaned + SCD2)")
    print("=" * 50)
    spark.read.parquet("data/silver/customers").show(truncate=False)

    print("\n" + "=" * 50)
    print("SILVER: Orders (cleaned)")
    print("=" * 50)
    spark.read.parquet("data/silver/orders").show(truncate=False)

    print("\n" + "=" * 50)
    print("SILVER: Leads (cleaned + SCD2)")
    print("=" * 50)
    spark.read.parquet("data/silver/leads").show(truncate=False)

    print("\n" + "=" * 50)
    print("GOLD: dim_customers")
    print("=" * 50)
    spark.read.parquet("data/gold/dim_customers").show(truncate=False)

    print("\n" + "=" * 50)
    print("GOLD: fact_orders")
    print("=" * 50)
    spark.read.parquet("data/gold/fact_orders").show(truncate=False)

    print("\n" + "=" * 50)
    print("GOLD: dim_leads")
    print("=" * 50)
    spark.read.parquet("data/gold/dim_leads").show(truncate=False)


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("medallion_pipeline") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    run_pipeline(spark)
    show_results(spark)