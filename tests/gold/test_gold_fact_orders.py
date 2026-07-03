import os
import pytest
from datetime import datetime
from pyspark.sql import DataFrame
from my_project.tasks.gold.fact_orders import FactOrdersTask


# ===========================
# SHARED TEST DATA
# ===========================

SINGLE_ORDER = [(1, 1, "LAPTOP", 999.99, "completed", "2024-01-15")]

SINGLE_CUSTOMER_IN_DIM = [
    (
        1001,
        1,
        "ALICE",
        "alice@email.com",
        "active",
        "UK",
        datetime(2024, 1, 1),
        datetime(9999, 12, 31),
        True,
    )
]

TWO_VERSIONS_OF_CUSTOMER = [
    (
        1001,
        1,
        "ALICE",
        "alice@old.com",
        "active",
        "UK",
        datetime(2024, 1, 1),
        datetime(2024, 6, 1),
        False,
    ),
    (
        1002,
        1,
        "ALICE",
        "alice@new.com",
        "active",
        "UK",
        datetime(2024, 6, 1),
        datetime(9999, 12, 31),
        True,
    ),
]

ORDER_WITH_NO_CUSTOMER = [(1, 99, "LAPTOP", 999.99, "completed", "2024-01-15")]


# ===========================
# TRANSFORM TESTS
# ===========================


class TestFactOrdersTransform:

    @pytest.fixture(scope="class")
    def dim_path(self, spark, tmp_path_factory, dim_customers_schema):
        """Write standard dim_customers once and reuse across tests"""
        path = str(tmp_path_factory.mktemp("dim_customers"))
        spark.createDataFrame(SINGLE_CUSTOMER_IN_DIM, dim_customers_schema).write.mode(
            "overwrite"
        ).parquet(path)
        return path

    @pytest.fixture(scope="class")
    def task(self, spark, dim_path):
        """Reusable task instance for transform tests"""
        return FactOrdersTask(
            spark=spark,
            input_path="unused",
            output_path="unused",
            dim_customers_path=dim_path,
        )

    @pytest.fixture(scope="class")
    def single_order_result(self, spark, task, silver_orders_schema):
        """Transform a single order — reused across multiple tests"""
        orders = spark.createDataFrame(SINGLE_ORDER, silver_orders_schema)
        return task.transform(orders).cache()

    def test_result_is_dataframe(self, single_order_result):
        """transform() should return a DataFrame"""
        assert isinstance(single_order_result, DataFrame)

    def test_customer_key_column_added(self, single_order_result):
        """fact_orders should have customer_key column"""
        assert "customer_key" in single_order_result.columns

    def test_customer_key_resolved(self, single_order_result):
        """customer_key should be resolved from dim_customers"""
        row = single_order_result.collect()[0]
        assert row["customer_key"] == 1001

    def test_all_order_columns_present(self, single_order_result):
        """All order columns should be present in fact_orders"""
        expected_columns = [
            "order_id",
            "customer_id",
            "product",
            "amount",
            "status",
            "order_date",
        ]
        for col in expected_columns:
            assert col in single_order_result.columns

    def test_joins_to_correct_customer_version(
        self, spark, dim_customers_schema, silver_orders_schema, tmp_path_factory
    ):
        """Order should join to customer version active at time of order"""
        dim_path = str(tmp_path_factory.mktemp("dim_v"))
        spark.createDataFrame(
            TWO_VERSIONS_OF_CUSTOMER, dim_customers_schema
        ).write.mode("overwrite").parquet(dim_path)

        task = FactOrdersTask(
            spark=spark,
            input_path="unused",
            output_path="unused",
            dim_customers_path=dim_path,
        )
        orders = spark.createDataFrame(
            [(1, 1, "LAPTOP", 999.99, "completed", "2024-06-15")], silver_orders_schema
        )

        result = task.transform(orders)
        assert result.count() == 1
        assert result.collect()[0]["customer_key"] == 1002

    def test_orders_without_customer_kept_with_null_key(
        self, spark, dim_customers_schema, silver_orders_schema, tmp_path_factory
    ):
        """Orders with no matching customer should have null customer_key"""
        dim_path = str(tmp_path_factory.mktemp("dim_null"))
        spark.createDataFrame(SINGLE_CUSTOMER_IN_DIM, dim_customers_schema).write.mode(
            "overwrite"
        ).parquet(dim_path)

        task = FactOrdersTask(
            spark=spark,
            input_path="unused",
            output_path="unused",
            dim_customers_path=dim_path,
        )
        orders = spark.createDataFrame(ORDER_WITH_NO_CUSTOMER, silver_orders_schema)
        result = task.transform(orders)
        assert result.count() == 1
        assert result.collect()[0]["customer_key"] is None


# ===========================
# FULL TASK RUN TESTS
# ===========================


class TestFactOrdersTaskRun:

    @pytest.fixture(scope="class")
    def task_output(
        self, spark, tmp_path_factory, silver_orders_schema, dim_customers_schema
    ):
        """Run FactOrdersTask once and reuse output across tests"""
        orders_path = str(tmp_path_factory.mktemp("orders"))
        dim_path = str(tmp_path_factory.mktemp("dim"))
        output_path = str(tmp_path_factory.mktemp("fact_orders"))

        spark.createDataFrame(SINGLE_ORDER, silver_orders_schema).write.mode(
            "overwrite"
        ).parquet(orders_path)

        spark.createDataFrame(SINGLE_CUSTOMER_IN_DIM, dim_customers_schema).write.mode(
            "overwrite"
        ).parquet(dim_path)

        FactOrdersTask(
            spark=spark,
            input_path=orders_path,
            output_path=output_path,
            dim_customers_path=dim_path,
        ).run()

        return {"spark": spark, "output_path": output_path}

    def test_output_path_created(self, task_output):
        """FactOrdersTask should create the output path"""
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
