import yaml
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DoubleType,
    BooleanType,
    LongType,
    TimestampType,
    DateType,
)
from my_project.utils.logger import get_logger
from my_project.utils.config_models import BronzeSchemaConfig

logger = get_logger(__name__)

TYPE_MAP = {
    "string": StringType(),
    "integer": IntegerType(),
    "float": FloatType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "long": LongType(),
    "timestamp": TimestampType(),
    "date": DateType(),
}


def load_schema_from_yaml(yaml_path: str) -> StructType:
    """
    Reads a YAML schema definition and returns a PySpark StructType.
    Validates the schema using Pydantic before building the StructType.

    Raises FileNotFoundError if the YAML file does not exist.
    Raises ValidationError if the schema is invalid.
    """
    try:
        with open(yaml_path, "r") as f:
            raw_config = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Schema YAML not found at path: {yaml_path}")

    # Validate with Pydantic — raises ValidationError if invalid
    config = BronzeSchemaConfig(**raw_config)

    fields = []
    for field in config.fields:
        fields.append(StructField(field.name, TYPE_MAP[field.type], field.nullable))

    logger.info(
        f"Loaded schema for table '{config.table}' "
        f"with {len(fields)} fields from: {yaml_path}"
    )

    return StructType(fields)
