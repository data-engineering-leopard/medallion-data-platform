import os
import pytest
from my_project.tasks.gold.dim_customers import DimCustomersTask, run_dim_customers


class TestBuildDimCustomers:

    @pytest.fixture(scope="class")
    def dim_result(self, spark, silver_customers_schema, tmp_path_factory):
        """Build dim_customers once and reuse across tests"""
        data = [(1, "ALICE", "alice@email.com", "active", "UK", None, None, True)]
        df = spark.createDataFrame(data, silver_customers_schema)
        task = DimCustomersTask(spark=spark, input_path="unused", output_path="unused")
        return task.transform(df).cache()

    def test_customer_key_is_added(self, dim_result):
        """dim_customers should have a customer_key column"""
        assert "customer_key" in dim_result.columns

    def test_customer_key_is_unique(self, spark, silver_customers_schema):
        """Every row in dim_customers should have a unique customer_key"""
        data = [
            (1, "ALICE", "alice@old.com", "active", "UK", None, None, False),
            (1, "ALICE", "alice@new.com", "active", "UK", None, None, True),
        ]
        df = spark.createDataFrame(data, silver_customers_schema)
        task = DimCustomersTask(spark=spark, input_path="unused", output_path="unused")
        result = task.transform(df)
        total = result.count()
        distinct = result.select("customer_key").distinct().count()
        assert total == distinct

    def test_all_silver_columns_present(self, dim_result):
        """All silver columns should be present in dim_customers"""
        assert "customer_id" in dim_result.columns
        assert "name" in dim_result.columns
        assert "email" in dim_result.columns
        assert "status" in dim_result.columns
        assert "country" in dim_result.columns
        assert "effective_from" in dim_result.columns
        assert "effective_to" in dim_result.columns
        assert "is_current" in dim_result.columns

    def test_id_renamed_to_customer_id(self, dim_result):
        """Silver 'id' column should be renamed to 'customer_id' in Gold"""
        assert "customer_id" in dim_result.columns
        assert "id" not in dim_result.columns

    def test_all_rows_preserved(self, spark, silver_customers_schema):
        """All rows including historical versions should be in dim_customers"""
        data = [
            (1, "ALICE", "alice@old.com", "active", "UK", None, None, False),
            (1, "ALICE", "alice@new.com", "active", "UK", None, None, True),
            (2, "BOB", "bob@email.com", "active", "US", None, None, True),
        ]
        df = spark.createDataFrame(data, silver_customers_schema)
        task = DimCustomersTask(spark=spark, input_path="unused", output_path="unused")
        result = task.transform(df)
        assert result.count() == 3


class TestRunDimCustomers:

    @pytest.fixture(scope="class")
    def dim_customers_output(self, spark, tmp_path_factory, silver_customers_schema):
        """Run dim_customers once and reuse output across tests"""
        silver_path = str(tmp_path_factory.mktemp("silver_customers"))
        gold_path = str(tmp_path_factory.mktemp("gold_dim_customers"))

        data = [(1, "ALICE", "alice@email.com", "active", "UK", None, None, True)]
        spark.createDataFrame(data, silver_customers_schema).write.mode(
            "overwrite"
        ).parquet(silver_path)

        run_dim_customers(spark, input_path=silver_path, output_path=gold_path)
        return {"spark": spark, "output_path": gold_path}

    def test_run_dim_customers_creates_output(self, dim_customers_output):
        """run_dim_customers should write parquet to output path"""
        assert os.path.exists(dim_customers_output["output_path"])

    def test_run_dim_customers_output_readable(self, dim_customers_output):
        """Output parquet should be readable with correct row count"""
        result = dim_customers_output["spark"].read.parquet(
            dim_customers_output["output_path"]
        )
        assert result.count() == 1
