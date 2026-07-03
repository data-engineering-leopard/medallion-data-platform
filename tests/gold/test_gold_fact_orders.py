import os
import pytest
from datetime import datetime
from my_project.tasks.gold.fact_orders import FactOrdersTask, run_fact_orders


class TestBuildFactOrders:

    @pytest.fixture(scope="class")
    def standard_fact_result(
        self, spark, silver_orders_schema, dim_customers_schema, tmp_path_factory
    ):
        """Build fact_orders once and reuse across tests"""
        dim_path = str(tmp_path_factory.mktemp("dim_customers"))
        spark.createDataFrame(
            [
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
            ],
            dim_customers_schema,
        ).write.mode("overwrite").parquet(dim_path)

        orders = spark.createDataFrame(
            [(1, 1, "LAPTOP", 999.99, "completed", "2024-01-15")], silver_orders_schema
        )

        task = FactOrdersTask(
            spark=spark,
            input_path="unused",
            output_path="unused",
            dim_customers_path=dim_path,
        )
        return task.transform(orders).cache()

    def test_customer_key_added_from_dim(self, standard_fact_result):
        """fact_orders should have customer_key joined from dim_customers"""
        assert "customer_key" in standard_fact_result.columns

    def test_all_order_columns_present(self, standard_fact_result):
        """All order columns should be present in fact_orders"""
        assert "order_id" in standard_fact_result.columns
        assert "customer_id" in standard_fact_result.columns
        assert "product" in standard_fact_result.columns
        assert "amount" in standard_fact_result.columns
        assert "status" in standard_fact_result.columns
        assert "order_date" in standard_fact_result.columns

    def test_order_joins_to_correct_customer_version(
        self, spark, silver_orders_schema, dim_customers_schema, tmp_path_factory
    ):
        """Order should join to the correct customer version at time of order"""
        dim_path = str(tmp_path_factory.mktemp("dim_v"))
        spark.createDataFrame(
            [
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
            ],
            dim_customers_schema,
        ).write.mode("overwrite").parquet(dim_path)

        orders = spark.createDataFrame(
            [(1, 1, "LAPTOP", 999.99, "completed", "2024-06-15")], silver_orders_schema
        )

        task = FactOrdersTask(
            spark=spark,
            input_path="unused",
            output_path="unused",
            dim_customers_path=dim_path,
        )
        result = task.transform(orders)
        assert result.count() == 1
        assert result.collect()[0]["customer_key"] == 1002

    def test_orders_without_matching_customer_kept(
        self, spark, silver_orders_schema, dim_customers_schema, tmp_path_factory
    ):
        """Orders with no matching customer should appear with null key"""
        dim_path = str(tmp_path_factory.mktemp("dim_null"))
        spark.createDataFrame(
            [
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
            ],
            dim_customers_schema,
        ).write.mode("overwrite").parquet(dim_path)

        orders = spark.createDataFrame(
            [(1, 99, "LAPTOP", 999.99, "completed", "2024-01-15")], silver_orders_schema
        )

        task = FactOrdersTask(
            spark=spark,
            input_path="unused",
            output_path="unused",
            dim_customers_path=dim_path,
        )
        result = task.transform(orders)
        assert result.count() == 1
        assert result.collect()[0]["customer_key"] is None


class TestRunFactOrders:

    @pytest.fixture(scope="class")
    def fact_orders_output(
        self, spark, tmp_path_factory, silver_orders_schema, dim_customers_schema
    ):
        """Run fact_orders once and reuse output across all run tests"""
        orders_path = str(tmp_path_factory.mktemp("orders"))
        dim_path = str(tmp_path_factory.mktemp("dim"))
        output_path = str(tmp_path_factory.mktemp("fact_orders"))

        spark.createDataFrame(
            [(1, 1, "LAPTOP", 999.99, "completed", "2024-01-15")], silver_orders_schema
        ).write.mode("overwrite").parquet(orders_path)

        spark.createDataFrame(
            [
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
            ],
            dim_customers_schema,
        ).write.mode("overwrite").parquet(dim_path)

        run_fact_orders(
            spark,
            orders_input_path=orders_path,
            dim_customers_path=dim_path,
            output_path=output_path,
        )
        return {"spark": spark, "output_path": output_path}

    def test_run_fact_orders_creates_output(self, fact_orders_output):
        """run_fact_orders should write parquet to output path"""
        assert os.path.exists(fact_orders_output["output_path"])

    def test_run_fact_orders_output_readable(self, fact_orders_output):
        """Output parquet should be readable with correct row count"""
        result = fact_orders_output["spark"].read.parquet(
            fact_orders_output["output_path"]
        )
        assert result.count() == 1
