from my_project.utils.logger import get_logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from my_project.utils.schema_loader import load_schema_from_yaml
from my_project.utils.schema_validator import validate_schema

logger = get_logger(__name__)

LEADS_SCHEMA_PATH = "config/schemas/bronze/salesforce_leads.yaml"


def get_leads_schema() -> StructType:
    """Loads the leads schema from YAML config"""
    return load_schema_from_yaml(LEADS_SCHEMA_PATH)


def load_leads(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Loads raw Salesforce leads data.
    Uses explicit schema from YAML.
    Leniently validates on load — logs warnings but does not fail.
    """
    schema = get_leads_schema()
    df = spark.read.csv(file_path, header=True, schema=schema)
    result = validate_schema(df, schema)

    if not result["is_valid"]:
        logger.warning(f"Leads schema drift detected: {result}")

    return df


def run_bronze_salesforce(
    spark: SparkSession,
    leads_input: str,
    output_path: str
) -> None:
    """
    Runs the full bronze task for Salesforce source system.
    Loads leads and writes to parquet output.
    """
    logger.info("Starting Bronze task for Salesforce")

    leads_df = load_leads(spark, leads_input)
    logger.info(f"Loaded {leads_df.count()} lead rows")

    leads_df.write.mode("overwrite").parquet(f"{output_path}/leads")
    logger.info(f"Written leads to {output_path}/leads")

    logger.info("Bronze task for Salesforce complete")


def main():
    from my_project.utils.logger import setup_logging
    setup_logging()

    import argparse
    parser = argparse.ArgumentParser(description="Bronze Salesforce task")
    parser.add_argument("--leads-input", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("bronze_salesforce") \
        .getOrCreate()

    run_bronze_salesforce(
        spark,
        leads_input=args.leads_input,
        output_path=args.output_path
    )


if __name__ == "__main__":
    main()