from my_project.utils.transformations import (
    filter_active_customers,
    uppercase_customer_names,
)


class TestFilterActiveCustomers:

    def test_only_active_customers_returned(self, spark):
        """Only rows with status 'active' should come back"""
        data = [("Alive", "active"), ("Bob", "inactive"), ("Charlie", "active")]

        df = spark.createDataFrame(data, ["name", "status"])

        result = filter_active_customers(df)

        assert result.count() == 2

    def test_empty_dataframe_returns_empty(self, spark):
        """An empty dataset should return an empty dataset"""
        from pyspark.sql.types import StructType, StructField, StringType

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )

        df = spark.createDataFrame([], schema)

        result = filter_active_customers(df)

        assert result.count() == 0


class TestUppercaseCustomerNames:

    def test_names_are_uppercased(self, spark):
        """Names should be converted to uppercase"""
        data = [("alice", "active"), ("bob", "active")]

        df = spark.createDataFrame(data, ["name", "status"])

        result = uppercase_customer_names(df)
        names = [row["name"] for row in result.collect()]

        assert "ALICE" in names
        assert "BOB" in names

    def test_already_uppercase_names_unchanged(self, spark):
        """Names already in uppercase should stay the same"""
        data = [("ALICE", "active")]
        df = spark.createDataFrame(data, ["name", "status"])

        result = uppercase_customer_names(df)
        names = [row["name"] for row in result.collect()]

        assert "ALICE" in names
