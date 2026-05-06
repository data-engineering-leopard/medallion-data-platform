import logging
from pyspark.sql import DataFrame, SparkSession
from my_project.utils.surrogate_key import add_surrogate_key

logger = logging.getLogger(__name__)


def build_dim_leads(silver_df: DataFrame) -> DataFrame:
    """
    Builds the dim_leads Gold dimension from Silver leads data.

    - Adds a surrogate key as the first column
    - Preserves all SCD2 columns from Silver
    - All historical versions are kept

    Args:
        silver_df: Cleaned Silver leads DataFrame with SCD2 columns

    Returns:
        dim_leads DataFrame ready for Gold output
    """
    logger.info("Building dim_leads")

    dim_df = add_surrogate_key(silver_df, "lead_key")

    logger.info(f"dim_leads built with {dim_df.count()} rows")

    return dim_df


def run_dim_leads(
    spark: SparkSession,
    input_path: str,
    output_path: str
) -> None:
    """
    Runs the dim_leads Gold task.

    Reads from Silver leads parquet, builds the dimension
    and writes to Gold parquet output.

    In Databricks this would write to a Delta table instead.

    Args:
        spark: Active SparkSession
        input_path: Path to Silver leads parquet
        output_path: Path to write Gold dim_leads parquet
    """
    logger.info("Running dim_leads task")
    logger.info(f"Reading Silver from: {input_path}")

    silver_df = spark.read.parquet(input_path)
    dim_df = build_dim_leads(silver_df)

    dim_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Written dim_leads to: {output_path}")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("gold_dim_leads") \
        .getOrCreate()

    run_dim_leads(
        spark,
        input_path="data/silver/leads",
        output_path="data/gold/dim_leads"
    )