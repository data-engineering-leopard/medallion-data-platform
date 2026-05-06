import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, LongType, BooleanType, TimestampType
)
from my_project.tasks.gold.fact_orders import (
    build_fact_orders,
    run_fact_orders
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("test_gold_fact_orders") \
        .getOrCreate()


@pytest.fixture
def silver_orders_schema():
    return StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("status", StringType(), True),
        StructField("order_date", StringType(), True)
    ])


@pytest.fixture
def dim_customers_schema():
    return StructType([
        StructField("customer_key", LongType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("status", StringType(), True),
        StructField("country", StringType(), True),
        StructField("effective_from", TimestampType(), True),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), True)
    ])


class TestBuildFactOrders:

    def test_customer_key_added_from_dim(
        self, spark, silver_orders_schema, dim_customers_schema
    ):
        """fact_orders should have customer_key joined from dim_customers"""
        orders = spark.createDataFrame([
            (1, 1, "LAPTOP", 999.99, "completed", "2024-01-15")
        ], silver_orders_schema)

        dim = spark.createDataFrame([
            (1001, 1, "ALICE", "alice@email.com", "active", "UK",
             None, None, True)
        ], dim_customers_schema)

        result = build_fact_orders(orders, dim)
        assert "customer_key" in result.columns

    def test_order_joins_to_current_customer_version(
            self, spark, silver_orders_schema, dim_customers_schema
    ):
        """Order should join to the correct customer version at time of order"""
        from datetime import datetime

        orders = spark.createDataFrame([
            (1, 1, "LAPTOP", 999.99, "completed", "2024-06-15")
        ], silver_orders_schema)

        dim = spark.createDataFrame([
            (1001, 1, "ALICE", "alice@old.com", "active", "UK",
             datetime(2024, 1, 1), datetime(2024, 6, 1), False),
            (1002, 1, "ALICE", "alice@new.com", "active", "UK",
             datetime(2024, 6, 1), datetime(9999, 12, 31), True)
        ], dim_customers_schema)

        result = build_fact_orders(orders, dim)

        assert result.count() == 1
        assert result.collect()[0]["customer_key"] == 1002

    def test_all_order_columns_present(
        self, spark, silver_orders_schema, dim_customers_schema
    ):
        """All order columns should be present in fact_orders"""
        orders = spark.createDataFrame([
            (1, 1, "LAPTOP", 999.99, "completed", "2024-01-15")
        ], silver_orders_schema)

        dim = spark.createDataFrame([
            (1001, 1, "ALICE", "alice@email.com", "active", "UK",
             None, None, True)
        ], dim_customers_schema)

        result = build_fact_orders(orders, dim)
        assert "order_id" in result.columns
        assert "customer_id" in result.columns
        assert "product" in result.columns
        assert "amount" in result.columns
        assert "status" in result.columns
        assert "order_date" in result.columns

    def test_orders_without_matching_customer_kept(
        self, spark, silver_orders_schema, dim_customers_schema
    ):
        """Orders with no matching customer should still appear with null key"""
        orders = spark.createDataFrame([
            (1, 99, "LAPTOP", 999.99, "completed", "2024-01-15")
        ], silver_orders_schema)

        dim = spark.createDataFrame([
            (1001, 1, "ALICE", "alice@email.com", "active", "UK",
             None, None, True)
        ], dim_customers_schema)

        result = build_fact_orders(orders, dim)
        assert result.count() == 1
        assert result.collect()[0]["customer_key"] is None


class TestRunFactOrders:

    def test_run_fact_orders_creates_output(
        self, spark, tmp_path,
        silver_orders_schema, dim_customers_schema
    ):
        """run_fact_orders should write parquet to output path"""
        orders_path = str(tmp_path / "silver/orders")
        dim_path = str(tmp_path / "gold/dim_customers")
        output_path = str(tmp_path / "gold/fact_orders")

        spark.createDataFrame(
            [(1, 1, "LAPTOP", 999.99, "completed", "2024-01-15")],
            silver_orders_schema
        ).write.parquet(orders_path)

        spark.createDataFrame(
            [(1001, 1, "ALICE", "alice@email.com", "active", "UK",
              None, None, True)],
            dim_customers_schema
        ).write.parquet(dim_path)

        run_fact_orders(
            spark,
            orders_input_path=orders_path,
            dim_customers_path=dim_path,
            output_path=output_path
        )

        assert os.path.exists(output_path)

    def test_run_fact_orders_output_readable(
        self, spark, tmp_path,
        silver_orders_schema, dim_customers_schema
    ):
        """Output parquet should be readable with correct row count"""
        orders_path = str(tmp_path / "silver/orders2")
        dim_path = str(tmp_path / "gold/dim_customers2")
        output_path = str(tmp_path / "gold/fact_orders2")

        spark.createDataFrame(
            [(1, 1, "LAPTOP", 999.99, "completed", "2024-01-15")],
            silver_orders_schema
        ).write.parquet(orders_path)

        spark.createDataFrame(
            [(1001, 1, "ALICE", "alice@email.com", "active", "UK",
              None, None, True)],
            dim_customers_schema
        ).write.parquet(dim_path)

        run_fact_orders(
            spark,
            orders_input_path=orders_path,
            dim_customers_path=dim_path,
            output_path=output_path
        )

        result = spark.read.parquet(output_path)
        assert result.count() == 1