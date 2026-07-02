from my_project.utils.logger import get_logger
import os
import glob
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from my_project.utils.scd2 import apply_scd2

logger = get_logger(__name__)


def load_silver_config(config_path: str) -> list:
    """
    Loads all YAML config files from the silver config folder.
    Each YAML file defines one silver table and its cleaning rules.
    Returns a list of config dicts, one per table.
    """
    yaml_files = glob.glob(f"{config_path}/*.yaml")

    if not yaml_files:
        raise FileNotFoundError(f"No YAML config files found in: {config_path}")

    from my_project.utils.config_models import SilverTableConfig

    configs = []
    for yaml_file in yaml_files:
        with open(yaml_file, "r") as f:
            raw_config = yaml.safe_load(f)
            # Validate with Pydantic — raises ValidationError if invalid
            config = SilverTableConfig(**raw_config)
            configs.append(config.model_dump())
            logger.info(f"Loaded and validated silver config: {yaml_file}")

    return configs


def apply_cleaning_rules(df: DataFrame, config: dict) -> DataFrame:
    """
    Applies cleaning rules defined in the silver YAML config.

    Supported rules:
    - drop_null_columns: remove rows where these columns are null
    - uppercase_columns: uppercase the values in these columns
    - lowercase_columns: lowercase the values in these columns
    """
    for col in config.get("drop_null_columns", []):
        df = df.filter(F.col(col).isNotNull())
        logger.info(f"Dropped nulls on column: {col}")

    for col in config.get("uppercase_columns", []):
        df = df.withColumn(col, F.upper(F.col(col)))
        logger.info(f"Uppercased column: {col}")

    for col in config.get("lowercase_columns", []):
        df = df.withColumn(col, F.lower(F.col(col)))
        logger.info(f"Lowercased column: {col}")

    return df


def run_silver_table(
    spark: SparkSession,
    config: dict,
    bronze_base_path: str = None,
    silver_base_path: str = None,
) -> None:
    """
    Runs the silver transformation for a single table.

    Reads from bronze parquet, applies SCD2 if configured
    (quarantine check and effective date happen before cleaning),
    applies cleaning rules, writes to silver parquet.
    Quarantined records are written to quarantine path.
    """
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
    quarantine_path = config.get("quarantine_path", None)

    logger.info(f"Running silver for table: {config['table']}")
    logger.info(f"Reading from: {input_path}")

    # Read from bronze
    incoming_df = spark.read.parquet(input_path)

    # Apply SCD2 or append only
    if config.get("scd2", False):
        logger.info(f"Applying SCD2 for table: {config['table']}")

        # Load existing silver data if it exists
        parquet_files = glob.glob(f"{output_path}/*.parquet")
        has_existing_data = os.path.exists(output_path) and len(parquet_files) > 0

        if has_existing_data:
            existing_df = spark.read.parquet(output_path)
            existing_df = existing_df.cache()
            existing_df.count()
        else:
            existing_df = spark.createDataFrame([], incoming_df.schema)

        # SCD2 runs on raw incoming data so date columns are still present
        scd2_result = apply_scd2(
            spark=spark,
            incoming_df=incoming_df,
            existing_df=existing_df,
            scd2_key=config["scd2_key"],
            track_columns=config["scd2_track_columns"],
            effective_from_column=config.get("effective_from_column", None),
            effective_from_fallback_column=config.get(
                "effective_from_fallback_column", None
            ),
            effective_from_ts=config.get("effective_from_ts", None),
        )

        # Apply cleaning rules to valid records after SCD2
        result_df = apply_cleaning_rules(scd2_result["valid"], config)
        quarantine_df = scd2_result["quarantine"]

        # Write quarantine records if any exist
        if quarantine_path and quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(quarantine_path)
            logger.warning(
                f"Written {quarantine_df.count()} quarantined records "
                f"to: {quarantine_path}"
            )

    else:
        logger.info(f"Append only for table: {config['table']}")
        result_df = apply_cleaning_rules(incoming_df, config)

    # Write to silver
    result_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Written silver to: {output_path}")


def run_silver(
    spark: SparkSession,
    config_path: str,
    bronze_base_path: str = None,
    silver_base_path: str = None,
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
            silver_base_path=silver_base_path,
        )

    logger.info(f"Silver task complete — " f"{len(configs)} tables processed")


def main():
    from my_project.utils.logger import setup_logging

    setup_logging()

    import argparse

    parser = argparse.ArgumentParser(description="Silver task")
    parser.add_argument(
        "--config-path",
        default=os.getenv("SILVER_CONFIG_PATH", "assets/silver"),
        help="Path to silver config YAML files",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("silver_task").getOrCreate()

    run_silver(spark, config_path=args.config_path)


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
