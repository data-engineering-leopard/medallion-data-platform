from my_project.utils.logger import get_logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from my_project.utils.schema_loader import load_schema_from_yaml
from my_project.utils.schema_validator import validate_schema

logger = get_logger(__name__)

# Paths to the YAML schema definitions for this source system
CUSTOMERS_SCHEMA_PATH = "config/schemas/bronze/online_tcg_customers.yaml"
ORDERS_SCHEMA_PATH = "config/schemas/bronze/online_tcg_orders.yaml"

# ===========================
# SCHEMAS
# ===========================

def get_customers_schema() -> StructType:
    """Loads the customers schema from YAML config"""
    return load_schema_from_yaml(CUSTOMERS_SCHEMA_PATH)


def get_orders_schema() -> StructType:
    """Loads the orders schema from YAML config"""
    return load_schema_from_yaml(ORDERS_SCHEMA_PATH)


# ===========================
# LOADERS
# ===========================

def load_customers(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Loads raw customers data from Online TCG source.
    Uses explicit schema from YAML.
    Leniently validates on load — logs warnings but does not fail.
    """
    schema = get_customers_schema()
    df = spark.read.csv(file_path, header=True, schema=schema)
    result = validate_schema(df, schema)

    if not result["is_valid"]:
        logger.warning(
            f"Customers schema drift detected: {result}"
        )

    return df


def load_orders(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Loads raw orders data from Online TCG source.
    Uses explicit schema from YAML.
    Leniently validates on load — logs warnings but does not fail.
    """
    schema = get_orders_schema()
    df = spark.read.csv(file_path, header=True, schema=schema)
    result = validate_schema(df, schema)

    if not result["is_valid"]:
        logger.warning(
            f"Orders schema drift detected: {result}"
        )

    return df


# ===========================
# TASK ENTRY POINT
# ===========================

def run_bronze(
    spark: SparkSession,
    customers_input: str,
    orders_input: str,
    output_path: str
) -> None:
    """
    Runs the full bronze task for Online TCG source system.
    Loads customers and orders, validates schemas leniently,
    and writes each to its own parquet output directory.

    Locally writes parquet files.
    In Databricks this would write to Delta tables instead.
    """
    logger.info("Starting Bronze task for Online TCG")

    customers_df = load_customers(spark, customers_input)
    logger.info(f"Loaded {customers_df.count()} customer rows")
    customers_df.write.mode("overwrite").parquet(
        f"{output_path}/customers"
    )
    logger.info(f"Written customers to {output_path}/customers")

    orders_df = load_orders(spark, orders_input)
    logger.info(f"Loaded {orders_df.count()} order rows")
    orders_df.write.mode("overwrite").parquet(
        f"{output_path}/orders"
    )
    logger.info(f"Written orders to {output_path}/orders")

    logger.info("Bronze task for Online TCG complete")


def main():
    from my_project.utils.logger import setup_logging
    setup_logging()

    import argparse
    parser = argparse.ArgumentParser(description="Bronze Online TCG task")
    parser.add_argument("--customers-input", required=True)
    parser.add_argument("--orders-input", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("bronze_online_tcg") \
        .getOrCreate()

    run_bronze(
        spark,
        customers_input=args.customers_input,
        orders_input=args.orders_input,
        output_path=args.output_path
    )


if __name__ == "__main__":
    main()