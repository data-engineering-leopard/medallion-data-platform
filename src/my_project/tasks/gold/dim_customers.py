from pyspark.sql import DataFrame, SparkSession

from my_project.tasks.core.base_gold_task import GoldTask
from my_project.utils.logger import get_logger
from my_project.utils.surrogate_key import add_surrogate_key

logger = get_logger(__name__)


class DimCustomersTask(GoldTask):
    """
    Gold task for building the dim_customers dimension table.

    Reads from Silver customers parquet, renames id to customer_id,
    adds a surrogate key and writes with SCD2 history tracking.

    load_type = scd2 — full history of customer changes is preserved.
    """

    load_type = "scd2"
    scd2_key = "customer_id"
    scd2_track_columns = ["email", "status", "country"]

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Renames id to customer_id and adds a surrogate key.

        Args:
            df: Silver customers DataFrame with SCD2 columns

        Returns:
            dim_customers DataFrame with surrogate key as first column
        """
        logger.info("Building dim_customers")
        df = df.withColumnRenamed("id", "customer_id")
        df = add_surrogate_key(df, "customer_key")
        logger.info(f"dim_customers built with {df.count()} rows")
        return df


def run_dim_customers(
    spark: SparkSession,
    input_path: str,
    output_path: str,
) -> None:
    """
    Entry point for the dim_customers Gold task.
    Instantiates and runs DimCustomersTask.

    Args:
        spark: Active SparkSession
        input_path: Path to Silver customers parquet
        output_path: Path to write Gold dim_customers parquet
    """
    task = DimCustomersTask(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )
    task.run()


def main():
    from my_project.utils.logger import setup_logging

    setup_logging()

    import argparse

    parser = argparse.ArgumentParser(description="Gold dim_customers task")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("gold_dim_customers").getOrCreate()

    run_dim_customers(
        spark,
        input_path=args.input_path,
        output_path=args.output_path,
    )


if __name__ == "__main__":
    main()
