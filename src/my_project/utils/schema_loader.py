import yaml
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DoubleType,
    BooleanType, LongType, TimestampType, DateType
)

# Maps the type names in YAML to actual PySpark types
TYPE_MAP = {
    "string": StringType(),
    "integer": IntegerType(),
    "float": FloatType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "long": LongType(),
    "timestamp": TimestampType(),
    "date": DateType()
}


def load_schema_from_yaml(yaml_path: str) -> StructType:
    """
    Reads a YAML schema definition and returns a PySpark StructType.

    Raises FileNotFoundError if the YAML file does not exist.
    Raises ValueError if an unknown type is found in the YAML.
    """
    try:
        with open(yaml_path, "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Schema YAML not found at path: {yaml_path}"
        )

    fields = []
    for field in config["fields"]:
        field_name = field["name"]
        field_type = field["type"].lower()
        nullable = field.get("nullable", True)

        if field_type not in TYPE_MAP:
            raise ValueError(
                f"Unknown type '{field_type}' for field '{field_name}'. "
                f"Supported types are: {list(TYPE_MAP.keys())}"
            )

        fields.append(
            StructField(field_name, TYPE_MAP[field_type], nullable)
        )

    return StructType(fields)