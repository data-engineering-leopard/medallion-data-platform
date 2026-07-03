import pytest
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from my_project.utils.test_data_builder import make_row, make_rows, make_dataframe


@pytest.fixture
def simple_schema():
    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("amount", FloatType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("score", LongType(), True),
            StructField("created_at", TimestampType(), True),
        ]
    )


# ===========================
# make_row TESTS
# ===========================


class TestMakeRow:

    def test_returns_a_tuple(self, simple_schema):
        """make_row should return a tuple"""
        result = make_row(simple_schema)
        assert isinstance(result, tuple)

    def test_tuple_length_matches_schema(self, simple_schema):
        """Tuple length should match number of fields in schema"""
        result = make_row(simple_schema)
        assert len(result) == len(simple_schema.fields)

    def test_integer_default_is_one(self, simple_schema):
        """IntegerType fields should default to 1"""
        result = make_row(simple_schema)
        assert result[0] == 1

    def test_string_default_is_not_empty(self, simple_schema):
        """StringType fields should have a non-empty default"""
        result = make_row(simple_schema)
        assert result[1] is not None
        assert len(result[1]) > 0

    def test_float_default_is_zero(self, simple_schema):
        """FloatType fields should default to 0.0"""
        result = make_row(simple_schema)
        assert result[3] == 0.0

    def test_boolean_default_is_true(self, simple_schema):
        """BooleanType fields should default to True"""
        result = make_row(simple_schema)
        assert result[4] is True

    def test_long_default_is_one(self, simple_schema):
        """LongType fields should default to 1"""
        result = make_row(simple_schema)
        assert result[5] == 1

    def test_timestamp_default_is_set(self, simple_schema):
        """TimestampType fields should have a default timestamp"""
        result = make_row(simple_schema)
        assert isinstance(result[6], datetime)

    def test_override_string_field(self, simple_schema):
        """Should override string field with provided value"""
        result = make_row(simple_schema, name="ALICE")
        assert result[1] == "ALICE"

    def test_override_integer_field(self, simple_schema):
        """Should override integer field with provided value"""
        result = make_row(simple_schema, id=42)
        assert result[0] == 42

    def test_override_boolean_field(self, simple_schema):
        """Should override boolean field with provided value"""
        result = make_row(simple_schema, is_active=False)
        assert result[4] is False

    def test_override_multiple_fields(self, simple_schema):
        """Should override multiple fields simultaneously"""
        result = make_row(simple_schema, name="BOB", status="inactive", id=99)
        assert result[0] == 99
        assert result[1] == "BOB"
        assert result[2] == "inactive"

    def test_override_with_none(self, simple_schema):
        """Should allow overriding a field with None"""
        result = make_row(simple_schema, name=None)
        assert result[1] is None

    def test_unknown_field_raises_error(self, simple_schema):
        """Should raise ValueError for unknown field names"""
        with pytest.raises(ValueError):
            make_row(simple_schema, unknown_field="value")


# ===========================
# make_rows TESTS
# ===========================


class TestMakeRows:

    def test_returns_list(self, simple_schema):
        """make_rows should return a list"""
        result = make_rows(simple_schema, [{}])
        assert isinstance(result, list)

    def test_returns_correct_number_of_rows(self, simple_schema):
        """make_rows should return one tuple per override dict"""
        result = make_rows(simple_schema, [{}, {}, {}])
        assert len(result) == 3

    def test_each_row_is_tuple(self, simple_schema):
        """Each item in result should be a tuple"""
        result = make_rows(simple_schema, [{}, {}])
        for row in result:
            assert isinstance(row, tuple)

    def test_overrides_applied_per_row(self, simple_schema):
        """Each row should have its own overrides applied"""
        result = make_rows(
            simple_schema,
            [
                {"name": "ALICE", "status": "active"},
                {"name": "BOB", "status": "inactive"},
            ],
        )
        assert result[0][1] == "ALICE"
        assert result[0][2] == "active"
        assert result[1][1] == "BOB"
        assert result[1][2] == "inactive"

    def test_rows_with_no_overrides_use_defaults(self, simple_schema):
        """Rows with empty override dict should use defaults"""
        result = make_rows(simple_schema, [{}])
        assert result[0][0] == 1
        assert result[0][4] is True

    def test_empty_overrides_list_returns_empty_list(self, simple_schema):
        """Empty list of overrides should return empty list"""
        result = make_rows(simple_schema, [])
        assert result == []


# ===========================
# make_dataframe TESTS
# ===========================


class TestMakeDataframe:

    def test_returns_dataframe(self, spark, simple_schema):
        """make_dataframe should return a PySpark DataFrame"""
        result = make_dataframe(spark, simple_schema, [{}])
        assert isinstance(result, DataFrame)

    def test_dataframe_has_correct_row_count(self, spark, simple_schema):
        """DataFrame should have one row per override dict"""
        result = make_dataframe(spark, simple_schema, [{}, {}, {}])
        assert result.count() == 3

    def test_dataframe_has_correct_columns(self, spark, simple_schema):
        """DataFrame should have all columns from schema"""
        result = make_dataframe(spark, simple_schema, [{}])
        for field in simple_schema.fields:
            assert field.name in result.columns

    def test_dataframe_schema_matches(self, spark, simple_schema):
        """DataFrame schema should match the provided schema"""
        result = make_dataframe(spark, simple_schema, [{}])
        assert result.schema == simple_schema

    def test_dataframe_overrides_applied(self, spark, simple_schema):
        """DataFrame should contain overridden values"""
        result = make_dataframe(
            spark, simple_schema, [{"name": "ALICE", "status": "active"}]
        )
        row = result.collect()[0]
        assert row["name"] == "ALICE"
        assert row["status"] == "active"

    def test_empty_overrides_returns_empty_dataframe(self, spark, simple_schema):
        """Empty list should return empty DataFrame with correct schema"""
        result = make_dataframe(spark, simple_schema, [])
        assert result.count() == 0
        assert result.schema == simple_schema
