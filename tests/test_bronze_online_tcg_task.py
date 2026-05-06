import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from my_project.tasks.bronze.bronze_online_tcg import (
    load_customers,
    load_orders,
    get_customers_schema,
    get_orders_schema,
    run_bronze
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("test_bronze_online_tcg") \
        .getOrCreate()


# ===========================
# SCHEMA TESTS
# ===========================

class TestSchemas:

    def test_customers_schema_loaded_from_yaml(self):
        """Customers schema should be loaded from YAML not hardcoded"""
        schema = get_customers_schema()
        assert isinstance(schema, StructType)

    def test_customers_schema_has_correct_columns(self):
        """Customers schema should define all expected columns"""
        schema = get_customers_schema()
        field_names = [field.name for field in schema.fields]
        assert "id" in field_names
        assert "name" in field_names
        assert "email" in field_names
        assert "status" in field_names
        assert "country" in field_names

    def test_orders_schema_loaded_from_yaml(self):
        """Orders schema should be loaded from YAML not hardcoded"""
        schema = get_orders_schema()
        assert isinstance(schema, StructType)

    def test_orders_schema_has_correct_columns(self):
        """Orders schema should define all expected columns"""
        schema = get_orders_schema()
        field_names = [field.name for field in schema.fields]
        assert "order_id" in field_names
        assert "customer_id" in field_names
        assert "product" in field_names
        assert "amount" in field_names
        assert "status" in field_names
        assert "order_date" in field_names


# ===========================
# CUSTOMERS LOADING TESTS
# ===========================

class TestLoadCustomers:

    def test_customers_loads_all_rows(self, spark):
        """Bronze should load all rows including dirty ones"""
        df = load_customers(spark, "data/raw/customers.csv")
        assert df.count() == 8

    def test_customers_has_correct_columns(self, spark):
        """Loaded customers should have all expected columns"""
        df = load_customers(spark, "data/raw/customers.csv")
        assert "id" in df.columns
        assert "name" in df.columns
        assert "email" in df.columns
        assert "status" in df.columns
        assert "country" in df.columns

    def test_customers_preserves_nulls(self, spark):
        """Bronze should NOT remove null values - that is Silver's job"""
        df = load_customers(spark, "data/raw/customers.csv")
        null_names = df.filter(df["name"].isNull()).count()
        assert null_names > 0

    def test_customers_preserves_mixed_case(self, spark):
        """Bronze should NOT normalise casing - that is Silver's job"""
        df = load_customers(spark, "data/raw/customers.csv")
        statuses = [row["status"] for row in df.collect()]
        assert "ACTIVE" in statuses

    def test_customers_enforces_schema(self, spark):
        """Customers should be loaded with schema from YAML"""
        df = load_customers(spark, "data/raw/customers.csv")
        assert df.schema == get_customers_schema()


# ===========================
# ORDERS LOADING TESTS
# ===========================

class TestLoadOrders:

    def test_orders_loads_all_rows(self, spark):
        """Bronze should load all rows including dirty ones"""
        df = load_orders(spark, "data/raw/orders.csv")
        assert df.count() == 8

    def test_orders_has_correct_columns(self, spark):
        """Loaded orders should have all expected columns"""
        df = load_orders(spark, "data/raw/orders.csv")
        assert "order_id" in df.columns
        assert "customer_id" in df.columns
        assert "product" in df.columns
        assert "amount" in df.columns
        assert "status" in df.columns
        assert "order_date" in df.columns

    def test_orders_preserves_nulls(self, spark):
        """Bronze should NOT remove null values - that is Silver's job"""
        df = load_orders(spark, "data/raw/orders.csv")
        null_dates = df.filter(df["order_date"].isNull()).count()
        assert null_dates > 0

    def test_orders_preserves_missing_customer_ids(self, spark):
        """Bronze should keep rows with missing customer IDs"""
        df = load_orders(spark, "data/raw/orders.csv")
        null_customers = df.filter(df["customer_id"].isNull()).count()
        assert null_customers > 0

    def test_orders_enforces_schema(self, spark):
        """Orders should be loaded with schema from YAML"""
        df = load_orders(spark, "data/raw/orders.csv")
        assert df.schema == get_orders_schema()


# ===========================
# BRONZE TASK RUN TESTS
# ===========================

class TestRunBronze:

    def test_run_bronze_creates_customers_output(self, spark, tmp_path):
        """run_bronze should write customers parquet to output path"""
        run_bronze(
            spark,
            customers_input="data/raw/customers.csv",
            orders_input="data/raw/orders.csv",
            output_path=str(tmp_path)
        )
        assert os.path.exists(f"{tmp_path}/customers")

    def test_run_bronze_creates_orders_output(self, spark, tmp_path):
        """run_bronze should write orders parquet to output path"""
        run_bronze(
            spark,
            customers_input="data/raw/customers.csv",
            orders_input="data/raw/orders.csv",
            output_path=str(tmp_path)
        )
        assert os.path.exists(f"{tmp_path}/orders")

    def test_run_bronze_customers_output_readable(self, spark, tmp_path):
        """Written customers parquet should be readable with correct row count"""
        run_bronze(
            spark,
            customers_input="data/raw/customers.csv",
            orders_input="data/raw/orders.csv",
            output_path=str(tmp_path)
        )
        df = spark.read.parquet(f"{tmp_path}/customers")
        assert df.count() == 8

    def test_run_bronze_orders_output_readable(self, spark, tmp_path):
        """Written orders parquet should be readable with correct row count"""
        run_bronze(
            spark,
            customers_input="data/raw/customers.csv",
            orders_input="data/raw/orders.csv",
            output_path=str(tmp_path)
        )
        df = spark.read.parquet(f"{tmp_path}/orders")
        assert df.count() == 8