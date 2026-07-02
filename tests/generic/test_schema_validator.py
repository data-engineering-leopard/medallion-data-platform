import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from my_project.utils.schema_validator import validate_schema


@pytest.fixture
def expected_schema():
    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
        ]
    )


class TestSchemaValidator:

    def test_returns_true_when_schema_matches(self, spark, expected_schema):
        """Should return True when data schema matches expected schema"""
        data = [(1, "Alice", "active")]
        df = spark.createDataFrame(data, expected_schema)
        result = validate_schema(df, expected_schema)
        assert result["is_valid"] is True

    def test_returns_false_when_column_missing(self, spark, expected_schema):
        """Should return False when a column is missing from the data"""
        partial_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                # status is missing
            ]
        )
        data = [(1, "Alice")]
        df = spark.createDataFrame(data, partial_schema)
        result = validate_schema(df, expected_schema)
        assert result["is_valid"] is False

    def test_identifies_missing_columns(self, spark, expected_schema):
        """Should list which columns are missing"""
        partial_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        data = [(1, "Alice")]
        df = spark.createDataFrame(data, partial_schema)
        result = validate_schema(df, expected_schema)
        assert "status" in result["missing_columns"]

    def test_returns_false_when_type_mismatch(self, spark, expected_schema):
        """Should return False when a column has the wrong data type"""
        wrong_schema = StructType(
            [
                StructField("id", StringType(), True),  # should be IntegerType
                StructField("name", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )
        data = [("1", "Alice", "active")]
        df = spark.createDataFrame(data, wrong_schema)
        result = validate_schema(df, expected_schema)
        assert result["is_valid"] is False

    def test_identifies_type_mismatches(self, spark, expected_schema):
        """Should list which columns have the wrong type"""
        wrong_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )
        data = [("1", "Alice", "active")]
        df = spark.createDataFrame(data, wrong_schema)
        result = validate_schema(df, expected_schema)
        assert "id" in result["type_mismatches"]

    def test_returns_empty_issues_when_valid(self, spark, expected_schema):
        """Should return empty lists when schema is valid"""
        data = [(1, "Alice", "active")]
        df = spark.createDataFrame(data, expected_schema)
        result = validate_schema(df, expected_schema)
        assert result["missing_columns"] == []
        assert result["type_mismatches"] == {}
