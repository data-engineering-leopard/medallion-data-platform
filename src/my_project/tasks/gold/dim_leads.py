from my_project.utils.logger import get_logger
from pyspark.sql import DataFrame, SparkSession
from my_project.utils.surrogate_key import add_surrogate_key

logger = get_logger(__name__)


def build_dim_leads(silver_df: DataFrame) -> DataFrame:
    """
    Builds the dim_leads Gold dimension from Silver leads data.
    Adds a surrogate key as the first column.
    All historical SCD2 versions are preserved.
    """
    logger.info("Building dim_leads")
    dim_df = add_surrogate_key(silver_df, "lead_key")
    logger.info(f"dim_leads built with {dim_df.count()} rows")
    return dim_df


def run_dim_leads(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    Runs the dim_leads Gold task.
    Reads from Silver leads parquet and writes to Gold parquet output.
    """
    logger.info("Running dim_leads task")
    logger.info(f"Reading Silver from: {input_path}")

    silver_df = spark.read.parquet(input_path)
    dim_df = build_dim_leads(silver_df)

    dim_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Written dim_leads to: {output_path}")


def main():
    from my_project.utils.logger import setup_logging

    setup_logging()

    import argparse

    parser = argparse.ArgumentParser(description="Gold dim_leads task")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("gold_dim_leads").getOrCreate()

    run_dim_leads(spark, input_path=args.input_path, output_path=args.output_path)


if __name__ == "__main__":
    main()
