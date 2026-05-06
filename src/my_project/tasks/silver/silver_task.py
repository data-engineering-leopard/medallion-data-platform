import logging
import os
import glob
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from my_project.utils.scd2 import apply_scd2

logger = logging.getLogger(__name__)


def load_silver_config(config_path: str) -> list:
    """
    Loads all YAML config files from the silver config folder.
    Each YAML file defines one silver table and its cleaning rules.
    Returns a list of config dicts, one per table.
    """
    yaml_files = glob.glob(f"{config_path}/*.yaml")

    if not yaml_files:
        raise FileNotFoundError(
            f"No YAML config files found in: {config_path}"
        )

    configs = []
    for yaml_file in yaml_files:
        with open(yaml_file, "r") as f:
            config = yaml.safe_load(f)
            configs.append(config)
            logger.info(f"Loaded silver config: {yaml_file}")

    return configs


def apply_cleaning_rules(df: DataFrame, config: dict) -> DataFrame:
    """
    Applies cleaning rules defined in the silver YAML config to a DataFrame.

    Supported rules:
    - drop_null_columns: remove rows where these columns are null
    - uppercase_columns: uppercase the values in these columns
    - lowercase_columns: lowercase the values in these columns
    """
    # Drop nulls on specified columns
    for col in config.get("drop_null_columns", []):
        df = df.filter(F.col(col).isNotNull())
        logger.info(f"Dropped nulls on column: {col}")

    # Uppercase specified columns
    for col in config.get("uppercase_columns", []):
        df = df.withColumn(col, F.upper(F.col(col)))
        logger.info(f"Uppercased column: {col}")

    # Lowercase specified columns
    for col in config.get("lowercase_columns", []):
        df = df.withColumn(col, F.lower(F.col(col)))
        logger.info(f"Lowercased column: {col}")

    return df


def run_silver_table(
    spark: SparkSession,
    config: dict,
    bronze_base_path: str = None,
    silver_base_path: str = None
) -> None:
    """
    Runs the silver transformation for a single table.

    Reads from bronze parquet, applies cleaning rules,
    applies SCD2 if configured, writes to silver parquet.

    bronze_base_path and silver_base_path override the paths
    in the config — used in tests to write to tmp_path.
    """
    # Allow path overrides for testing
    input_path = (
        f"{bronze_base_path}/{config['table']}"
        if bronze_base_path
        else config["input_path"]
    )
    output_path = (
        f"{silver_base_path}/{config['table']}"
        if silver_base_path
        else config["output_path"]
    )

    logger.info(f"Running silver for table: {config['table']}")
    logger.info(f"Reading from: {input_path}")

    # Read from bronze
    incoming_df = spark.read.parquet(input_path)

    # Apply cleaning rules
    cleaned_df = apply_cleaning_rules(incoming_df, config)

    # Apply SCD2 or append only
    if config.get("scd2", False):
        logger.info(f"Applying SCD2 for table: {config['table']}")

        # Load existing silver data if it exists
        if os.path.exists(output_path):
            existing_df = spark.read.parquet(output_path)
        else:
            existing_df = spark.createDataFrame([], cleaned_df.schema)

        result_df = apply_scd2(
            spark=spark,
            incoming_df=cleaned_df,
            existing_df=existing_df,
            scd2_key=config["scd2_key"],
            track_columns=config["scd2_track_columns"]
        )
    else:
        logger.info(
            f"Append only for table: {config['table']}"
        )
        result_df = cleaned_df

    # Write to silver
    result_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Written silver to: {output_path}")


def run_silver(
    spark: SparkSession,
    config_path: str,
    bronze_base_path: str = None,
    silver_base_path: str = None
) -> None:
    """
    Runs the full silver task for all tables defined in config.
    Adding a new silver table = add a new YAML file, no code changes.
    """
    logger.info("Starting Silver task")

    configs = load_silver_config(config_path)

    for config in configs:
        run_silver_table(
            spark=spark,
            config=config,
            bronze_base_path=bronze_base_path,
            silver_base_path=silver_base_path
        )

    logger.info(
        f"Silver task complete — "
        f"{len(configs)} tables processed"
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("silver_task") \
        .getOrCreate()

    run_silver(
        spark,
        config_path="config/schemas/silver"
    )