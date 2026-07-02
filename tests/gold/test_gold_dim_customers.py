import os
from my_project.tasks.gold.dim_customers import build_dim_customers, run_dim_customers


class TestBuildDimCustomers:

    def test_customer_key_is_added(self, spark, silver_customers_schema):
        """dim_customers should have a customer_key column"""
        data = [(1, "ALICE", "alice@email.com", "active", "UK", None, None, True)]
        df = spark.createDataFrame(data, silver_customers_schema)
        result = build_dim_customers(df)
        assert "customer_key" in result.columns

    def test_customer_key_is_unique(self, spark, silver_customers_schema):
        """Every row in dim_customers should have a unique customer_key"""
        data = [
            (1, "ALICE", "alice@old.com", "active", "UK", None, None, False),
            (1, "ALICE", "alice@new.com", "active", "UK", None, None, True),
        ]
        df = spark.createDataFrame(data, silver_customers_schema)
        result = build_dim_customers(df)
        total = result.count()
        distinct = result.select("customer_key").distinct().count()
        assert total == distinct

    def test_all_silver_columns_present(self, spark, silver_customers_schema):
        """All silver columns should be present in dim_customers"""
        data = [(1, "ALICE", "alice@email.com", "active", "UK", None, None, True)]
        df = spark.createDataFrame(data, silver_customers_schema)
        result = build_dim_customers(df)
        assert "customer_id" in result.columns
        assert "name" in result.columns
        assert "email" in result.columns
        assert "status" in result.columns
        assert "country" in result.columns
        assert "effective_from" in result.columns
        assert "effective_to" in result.columns
        assert "is_current" in result.columns

    def test_id_renamed_to_customer_id(self, spark, silver_customers_schema):
        """Silver 'id' column should be renamed to 'customer_id' in Gold"""
        data = [(1, "ALICE", "alice@email.com", "active", "UK", None, None, True)]
        df = spark.createDataFrame(data, silver_customers_schema)
        result = build_dim_customers(df)
        assert "customer_id" in result.columns
        assert "id" not in result.columns

    def test_all_rows_preserved(self, spark, silver_customers_schema):
        """All rows including historical versions should be in dim_customers"""
        data = [
            (1, "ALICE", "alice@old.com", "active", "UK", None, None, False),
            (1, "ALICE", "alice@new.com", "active", "UK", None, None, True),
            (2, "BOB", "bob@email.com", "active", "US", None, None, True),
        ]
        df = spark.createDataFrame(data, silver_customers_schema)
        result = build_dim_customers(df)
        assert result.count() == 3


class TestRunDimCustomers:

    def test_run_dim_customers_creates_output(
        self, spark, tmp_path, silver_customers_schema
    ):
        """run_dim_customers should write parquet to output path"""
        silver_path = str(tmp_path / "silver/customers")
        gold_path = str(tmp_path / "gold/dim_customers")

        data = [(1, "ALICE", "alice@email.com", "active", "UK", None, None, True)]
        spark.createDataFrame(data, silver_customers_schema).write.parquet(silver_path)

        run_dim_customers(spark, input_path=silver_path, output_path=gold_path)

        assert os.path.exists(gold_path)

    def test_run_dim_customers_output_readable(
        self, spark, tmp_path, silver_customers_schema
    ):
        """Output parquet should be readable with correct row count"""
        silver_path = str(tmp_path / "silver/customers2")
        gold_path = str(tmp_path / "gold/dim_customers2")

        data = [(1, "ALICE", "alice@email.com", "active", "UK", None, None, True)]
        spark.createDataFrame(data, silver_customers_schema).write.parquet(silver_path)

        run_dim_customers(spark, input_path=silver_path, output_path=gold_path)

        result = spark.read.parquet(gold_path)
        assert result.count() == 1
