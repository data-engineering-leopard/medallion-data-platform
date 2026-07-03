from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)
from my_project.utils.logger import get_logger

logger = get_logger(__name__)

# ===========================
# DEFAULT VALUES PER TYPE
# ===========================

DEFAULTS = {
    IntegerType: 1,
    LongType: 1,
    FloatType: 0.0,
    DoubleType: 0.0,
    StringType: "default",
    BooleanType: True,
    TimestampType: datetime(2024, 1, 1, 0, 0, 0),
    DateType: datetime(2024, 1, 1).date(),
}


def _get_default(field_type) -> object:
    """
    Returns the default value for a given PySpark field type.
    Falls back to None for unknown types.
    """
    for type_class, default in DEFAULTS.items():
        if isinstance(field_type, type_class):
            return default
    logger.warning(
        f"No default found for type {type(field_type).__name__} — using None"
    )
    return None


def make_row(schema: StructType, **overrides) -> tuple:
    """
    Creates a single data row as a tuple matching the given schema.

    Unspecified fields are filled with sensible defaults based on
    their PySpark data type. Specified fields override the defaults.

    Args:
        schema: PySpark StructType defining the row structure
        **overrides: Field name/value pairs to override defaults

    Returns:
        A tuple of values matching the schema field order

    Raises:
        ValueError: If an override field name is not in the schema

    Example:
        row = make_row(
            silver_customers_schema,
            name="ALICE",
            status="active"
        )
    """
    field_names = [field.name for field in schema.fields]

    # Validate all override keys exist in schema
    for key in overrides:
        if key not in field_names:
            raise ValueError(
                f"Field '{key}' is not in the schema. "
                f"Available fields: {field_names}"
            )

    row = []
    for field in schema.fields:
        if field.name in overrides:
            row.append(overrides[field.name])
        else:
            row.append(_get_default(field.dataType))

    return tuple(row)


def make_rows(schema: StructType, overrides_list: list) -> list:
    """
    Creates multiple data rows as a list of tuples matching the given schema.

    Each item in overrides_list is a dict of field overrides for that row.
    Unspecified fields use defaults based on their PySpark data type.

    Args:
        schema: PySpark StructType defining the row structure
        overrides_list: List of dicts, one per row, with field overrides

    Returns:
        List of tuples, one per item in overrides_list

    Example:
        rows = make_rows(silver_customers_schema, [
            {"name": "ALICE", "status": "active"},
            {"name": "BOB", "status": "inactive"}
        ])
    """
    return [make_row(schema, **overrides) for overrides in overrides_list]


def make_dataframe(
    spark: SparkSession, schema: StructType, overrides_list: list
) -> DataFrame:
    """
    Creates a PySpark DataFrame matching the given schema.

    Each item in overrides_list is a dict of field overrides for that row.
    Unspecified fields use defaults based on their PySpark data type.

    Args:
        spark: Active SparkSession
        schema: PySpark StructType defining the DataFrame structure
        overrides_list: List of dicts, one per row, with field overrides

    Returns:
        PySpark DataFrame with the given schema and data

    Example:
        df = make_dataframe(spark, silver_customers_schema, [
            {"name": "ALICE", "status": "active"},
            {"name": "BOB", "status": "inactive"}
        ])
    """
    rows = make_rows(schema, overrides_list)
    return spark.createDataFrame(rows, schema)
