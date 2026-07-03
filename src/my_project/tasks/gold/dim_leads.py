from pyspark.sql import DataFrame, SparkSession

from my_project.tasks.core.base_gold_task import GoldTask
from my_project.utils.logger import get_logger
from my_project.utils.surrogate_key import add_surrogate_key

logger = get_logger(__name__)


class DimLeadsTask(GoldTask):
    """
    Gold task for building the dim_leads dimension table.

    Reads from Silver leads parquet, adds a surrogate key
    and writes with SCD2 history tracking.

    load_type = scd2 — full history of lead changes is preserved.
    """

    load_type = "scd2"
    scd2_key = "lead_id"
    scd2_track_columns = ["status", "email", "country"]

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Adds a surrogate key to the leads dimension.

        Args:
            df: Silver leads DataFrame with SCD2 columns

        Returns:
            dim_leads DataFrame with surrogate key as first column
        """
        logger.info("Building dim_leads")
        df = add_surrogate_key(df, "lead_key")
        logger.info(f"dim_leads built with {df.count()} rows")
        return df


def run_dim_leads(
    spark: SparkSession,
    input_path: str,
    output_path: str,
) -> None:
    """
    Entry point for the dim_leads Gold task.
    Instantiates and runs DimLeadsTask.

    Args:
        spark: Active SparkSession
        input_path: Path to Silver leads parquet
        output_path: Path to write Gold dim_leads parquet
    """
    task = DimLeadsTask(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )
    task.run()


def main():
    from my_project.utils.logger import setup_logging

    setup_logging()

    import argparse

    parser = argparse.ArgumentParser(description="Gold dim_leads task")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("gold_dim_leads").getOrCreate()

    run_dim_leads(
        spark,
        input_path=args.input_path,
        output_path=args.output_path,
    )


if __name__ == "__main__":
    main()
