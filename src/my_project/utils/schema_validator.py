from my_project.utils.logger import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

logger = get_logger(__name__)


def validate_schema(df: DataFrame, expected_schema: StructType) -> dict:
    """
    Leniently validates a DataFrame against an expected schema.

    Does NOT raise errors — instead returns a validation result dict
    so the caller can decide what to do with any issues found.

    Returns a dict with:
        - is_valid: bool
        - missing_columns: list of column names missing from the data
        - type_mismatches: dict of column name -> (expected type, actual type)
    """
    actual_fields = {field.name: field.dataType for field in df.schema.fields}
    expected_fields = {field.name: field.dataType for field in expected_schema.fields}

    missing_columns = []
    type_mismatches = {}

    for field_name, expected_type in expected_fields.items():
        if field_name not in actual_fields:
            # Column is missing entirely from the incoming data
            missing_columns.append(field_name)
            logger.warning(
                f"Missing column: '{field_name}' " f"(expected type: {expected_type})"
            )
        elif type(actual_fields[field_name]) != type(expected_type):
            # Column exists but has the wrong type
            type_mismatches[field_name] = {
                "expected": str(expected_type),
                "actual": str(actual_fields[field_name]),
            }
            logger.warning(
                f"Type mismatch for column '{field_name}': "
                f"expected {expected_type}, got {actual_fields[field_name]}"
            )

    is_valid = len(missing_columns) == 0 and len(type_mismatches) == 0

    if is_valid:
        logger.info("Schema validation passed")
    else:
        logger.warning(
            f"Schema validation failed — "
            f"missing columns: {missing_columns}, "
            f"type mismatches: {type_mismatches}"
        )

    return {
        "is_valid": is_valid,
        "missing_columns": missing_columns,
        "type_mismatches": type_mismatches,
    }
