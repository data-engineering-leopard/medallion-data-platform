from my_project.utils.logger import get_logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = get_logger(__name__)


def build_fact_orders(orders_df: DataFrame, dim_customers_df: DataFrame) -> DataFrame:
    """
    Builds the fact_orders Gold fact table.

    Joins Silver orders to dim_customers at the time of the order.
    This means the customer_key links to the version of the customer
    that was active when the order was placed — not necessarily
    the current version.

    If no matching customer version is found the order is still
    kept with a null customer_key — we never drop facts.

    Args:
        orders_df: Silver orders DataFrame
        dim_customers_df: Gold dim_customers DataFrame with SCD2 columns

    Returns:
        fact_orders DataFrame ready for Gold output
    """
    logger.info("Building fact_orders")

    # Cast order_date to timestamp for date range comparison
    orders_with_ts = orders_df.withColumn(
        "order_date_ts", F.to_timestamp(F.col("order_date"), "yyyy-MM-dd")
    )

    # Join to the customer version that was active at time of order
    # An order falls within a customer version when:
    # order_date >= effective_from AND order_date < effective_to
    fact_df = orders_with_ts.join(
        dim_customers_df,
        on=(
            (orders_with_ts["customer_id"] == dim_customers_df["customer_id"])
            & (orders_with_ts["order_date_ts"] >= dim_customers_df["effective_from"])
            & (orders_with_ts["order_date_ts"] < dim_customers_df["effective_to"])
        ),
        how="left",
    ).select(
        orders_df["order_id"],
        dim_customers_df["customer_key"],
        orders_df["customer_id"],
        orders_df["product"],
        orders_df["amount"],
        orders_df["status"],
        orders_df["order_date"],
    )

    logger.info(f"fact_orders built with {fact_df.count()} rows")

    return fact_df


def run_fact_orders(
    spark: SparkSession,
    orders_input_path: str,
    dim_customers_path: str,
    output_path: str,
) -> None:
    """
    Runs the fact_orders Gold task.

    Reads Silver orders and Gold dim_customers, builds the fact
    table and writes to Gold parquet output.

    In Databricks this would write to a Delta table instead.

    Args:
        spark: Active SparkSession
        orders_input_path: Path to Silver orders parquet
        dim_customers_path: Path to Gold dim_customers parquet
        output_path: Path to write Gold fact_orders parquet
    """
    logger.info("Running fact_orders task")
    logger.info(f"Reading Silver orders from: {orders_input_path}")
    logger.info(f"Reading dim_customers from: {dim_customers_path}")

    orders_df = spark.read.parquet(orders_input_path)
    dim_customers_df = spark.read.parquet(dim_customers_path)

    fact_df = build_fact_orders(orders_df, dim_customers_df)

    fact_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Written fact_orders to: {output_path}")


def main():
    from my_project.utils.logger import setup_logging

    setup_logging()

    import argparse

    parser = argparse.ArgumentParser(description="Gold fact_orders task")
    parser.add_argument("--orders-input-path", required=True)
    parser.add_argument("--dim-customers-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("gold_fact_orders").getOrCreate()

    run_fact_orders(
        spark,
        orders_input_path=args.orders_input_path,
        dim_customers_path=args.dim_customers_path,
        output_path=args.output_path,
    )


if __name__ == "__main__":
    main()
