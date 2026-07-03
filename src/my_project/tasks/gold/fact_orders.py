from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from my_project.tasks.core.base_gold_task import GoldTask
from my_project.utils.logger import get_logger

logger = get_logger(__name__)


class FactOrdersTask(GoldTask):
    """
    Gold task for building the fact_orders fact table.

    Reads from Silver orders parquet and joins to dim_customers
    at the time of the order for historical accuracy.

    load_type = overwrite — orders are immutable, full refresh each run.
    """

    load_type = "overwrite"

    def __init__(
        self,
        spark: SparkSession,
        input_path: str,
        output_path: str,
        dim_customers_path: str,
    ):
        super().__init__(spark, input_path, output_path)
        self.dim_customers_path = dim_customers_path

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Joins orders to dim_customers at the time of the order.

        Loads dim_customers internally since fact_orders requires
        two input sources. The join uses order_date to find the
        correct customer version that was active at time of order.

        Args:
            df: Silver orders DataFrame

        Returns:
            fact_orders DataFrame with customer_key resolved
        """
        logger.info("Building fact_orders")

        dim_customers_df = self.spark.read.parquet(self.dim_customers_path)

        orders_with_ts = df.withColumn(
            "order_date_ts",
            F.to_timestamp(F.col("order_date"), "yyyy-MM-dd"),
        )

        fact_df = orders_with_ts.join(
            dim_customers_df,
            on=(
                (orders_with_ts["customer_id"] == dim_customers_df["customer_id"])
                & (
                    orders_with_ts["order_date_ts"]
                    >= dim_customers_df["effective_from"]
                )
                & (orders_with_ts["order_date_ts"] < dim_customers_df["effective_to"])
            ),
            how="left",
        ).select(
            df["order_id"],
            dim_customers_df["customer_key"],
            df["customer_id"],
            df["product"],
            df["amount"],
            df["status"],
            df["order_date"],
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
    Entry point for the fact_orders Gold task.
    Instantiates and runs FactOrdersTask.

    Args:
        spark: Active SparkSession
        orders_input_path: Path to Silver orders parquet
        dim_customers_path: Path to Gold dim_customers parquet
        output_path: Path to write Gold fact_orders parquet
    """
    task = FactOrdersTask(
        spark=spark,
        input_path=orders_input_path,
        output_path=output_path,
        dim_customers_path=dim_customers_path,
    )
    task.run()


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
