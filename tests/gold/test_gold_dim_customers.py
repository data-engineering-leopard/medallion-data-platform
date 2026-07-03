import os
import pytest
from pyspark.sql import DataFrame
from my_project.tasks.gold.dim_customers import DimCustomersTask


# ===========================
# SHARED TEST DATA
# ===========================

SINGLE_CUSTOMER = [(1, "ALICE", "alice@email.com", "active", "UK", None, None, True)]

TWO_VERSIONS_OF_CUSTOMER = [
    (1, "ALICE", "alice@old.com", "active", "UK", None, None, False),
    (1, "ALICE", "alice@new.com", "active", "UK", None, None, True),
]

MULTIPLE_CUSTOMERS = TWO_VERSIONS_OF_CUSTOMER + [
    (2, "BOB", "bob@email.com", "active", "US", None, None, True)
]


# ===========================
# TRANSFORM TESTS
# ===========================


class TestDimCustomersTransform:

    @pytest.fixture(scope="class")
    def task(self, spark):
        """Reusable task instance for transform tests"""
        return DimCustomersTask(spark=spark, input_path="unused", output_path="unused")

    @pytest.fixture(scope="class")
    def single_customer_result(self, spark, task, silver_customers_schema):
        """Transform a single customer — reused across multiple tests"""
        df = spark.createDataFrame(SINGLE_CUSTOMER, silver_customers_schema)
        return task.transform(df).cache()

    def test_customer_key_is_added(self, single_customer_result):
        """dim_customers should have a customer_key column"""
        assert "customer_key" in single_customer_result.columns

    def test_result_is_dataframe(self, single_customer_result):
        """transform() should return a DataFrame"""
        assert isinstance(single_customer_result, DataFrame)

    def test_customer_key_is_unique(self, spark, task, silver_customers_schema):
        """Every row in dim_customers should have a unique customer_key"""
        df = spark.createDataFrame(TWO_VERSIONS_OF_CUSTOMER, silver_customers_schema)
        result = task.transform(df)
        total = result.count()
        distinct = result.select("customer_key").distinct().count()
        assert total == distinct

    def test_customer_key_is_not_null(self, single_customer_result):
        """customer_key should never be null"""
        null_count = single_customer_result.filter(
            single_customer_result["customer_key"].isNull()
        ).count()
        assert null_count == 0

    def test_customer_key_is_first_column(self, single_customer_result):
        """customer_key should be the first column"""
        assert single_customer_result.columns[0] == "customer_key"

    def test_id_renamed_to_customer_id(self, single_customer_result):
        """Silver 'id' column should be renamed to 'customer_id' in Gold"""
        assert "customer_id" in single_customer_result.columns
        assert "id" not in single_customer_result.columns

    def test_all_silver_columns_present(self, single_customer_result):
        """All silver columns should be present in dim_customers"""
        expected_columns = [
            "customer_id",
            "name",
            "email",
            "status",
            "country",
            "effective_from",
            "effective_to",
            "is_current",
        ]
        for col in expected_columns:
            assert col in single_customer_result.columns

    def test_all_rows_preserved(self, spark, task, silver_customers_schema):
        """All rows including historical versions should be in dim_customers"""
        df = spark.createDataFrame(MULTIPLE_CUSTOMERS, silver_customers_schema)
        result = task.transform(df)
        assert result.count() == 3


# ===========================
# FULL TASK RUN TESTS
# ===========================


class TestDimCustomersTaskRun:

    @pytest.fixture(scope="class")
    def task_output(self, spark, tmp_path_factory, silver_customers_schema):
        """Run DimCustomersTask once and reuse output across tests"""
        silver_path = str(tmp_path_factory.mktemp("silver_customers"))
        gold_path = str(tmp_path_factory.mktemp("gold_dim_customers"))

        spark.createDataFrame(SINGLE_CUSTOMER, silver_customers_schema).write.mode(
            "overwrite"
        ).parquet(silver_path)

        DimCustomersTask(
            spark=spark, input_path=silver_path, output_path=gold_path
        ).run()

        return {"spark": spark, "output_path": gold_path}

    def test_output_path_created(self, task_output):
        """DimCustomersTask should create the output path"""
        assert os.path.exists(task_output["output_path"])

    def test_output_is_readable(self, task_output):
        """Output parquet should be readable"""
        result = task_output["spark"].read.parquet(task_output["output_path"])
        assert isinstance(result, DataFrame)

    def test_output_has_correct_row_count(self, task_output):
        """Output should contain correct number of rows"""
        result = task_output["spark"].read.parquet(task_output["output_path"])
        assert result.count() == 1

    def test_output_has_customer_key_column(self, task_output):
        """Output parquet should have customer_key column"""
        result = task_output["spark"].read.parquet(task_output["output_path"])
        assert "customer_key" in result.columns

    def test_output_has_customer_id_not_id(self, task_output):
        """Output should have customer_id not id"""
        result = task_output["spark"].read.parquet(task_output["output_path"])
        assert "customer_id" in result.columns
        assert "id" not in result.columns
