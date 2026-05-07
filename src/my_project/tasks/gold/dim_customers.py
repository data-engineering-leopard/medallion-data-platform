from my_project.utils.logger import get_logger
from pyspark.sql import DataFrame, SparkSession
from my_project.utils.surrogate_key import add_surrogate_key

logger = get_logger(__name__)

def build_dim_customers(silver_df: DataFrame) -> DataFrame:
    """
    Builds the dim_customers Gold dimension from Silver customers data.

    - Renames 'id' to 'customer_id' for clarity
    - Adds a surrogate key as the first column
    - Preserves all SCD2 columns from Silver
    - All historical versions are kept

    Args:
        silver_df: Cleaned Silver customers DataFrame with SCD2 columns

    Returns:
        dim_customers DataFrame ready for Gold output
    """
    logger.info("Building dim_customers")

    # Rename 'id' to 'customer_id' for dimensional model clarity
    dim_df = silver_df.withColumnRenamed("id", "customer_id")

    # Add surrogate key as first column
    dim_df = add_surrogate_key(dim_df, "customer_key")

    logger.info(f"dim_customers built with {dim_df.count()} rows")

    return dim_df


def run_dim_customers(
    spark: SparkSession,
    input_path: str,
    output_path: str
) -> None:
    """
    Runs the dim_customers Gold task.

    Reads from Silver customers parquet, builds the dimension
    and writes to Gold parquet output.

    In Databricks this would write to a Delta table instead.

    Args:
        spark: Active SparkSession
        input_path: Path to Silver customers parquet
        output_path: Path to write Gold dim_customers parquet
    """
    logger.info(f"Running dim_customers task")
    logger.info(f"Reading Silver from: {input_path}")

    silver_df = spark.read.parquet(input_path)
    dim_df = build_dim_customers(silver_df)

    dim_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Written dim_customers to: {output_path}")

def main():
    from my_project.utils.logger import setup_logging
    setup_logging()

    import argparse
    parser = argparse.ArgumentParser(description="Gold dim_customers task")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("gold_dim_customers") \
        .getOrCreate()

    run_dim_customers(
        spark,
        input_path=args.input_path,
        output_path=args.output_path
    )


if __name__ == "__main__":
    main()