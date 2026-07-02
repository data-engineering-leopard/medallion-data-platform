import pytest
import os
from pyspark.sql.types import StructType
from my_project.tasks.bronze.bronze_online_tcg import (
    load_customers,
    load_orders,
    get_customers_schema,
    get_orders_schema,
    run_bronze,
)


# ===========================
# SESSION FIXTURES
# ===========================


@pytest.fixture(scope="session")
def customers_df(spark):
    """Load customers CSV once for the entire test session"""
    return load_customers(spark, "data/raw/customers.csv").cache()


@pytest.fixture(scope="session")
def customers_data(customers_df):
    """Collect customer rows once for the entire test session"""
    return customers_df.collect()


@pytest.fixture(scope="session")
def orders_df(spark):
    """Load orders CSV once for the entire test session"""
    return load_orders(spark, "data/raw/orders.csv").cache()


@pytest.fixture(scope="session")
def orders_data(orders_df):
    """Collect order rows once for the entire test session"""
    return orders_df.collect()


# ===========================
# CUSTOMERS SCHEMA TESTS
# ===========================


class TestCustomersSchema:

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
        assert "created_date" in field_names
        assert "updated_date" in field_names


# ===========================
# CUSTOMERS LOADING TESTS
# ===========================


class TestLoadCustomers:

    def test_customers_loads_all_rows(self, customers_df):
        """Bronze should load all rows including dirty ones"""
        assert customers_df.count() == 8

    def test_customers_has_correct_columns(self, customers_df):
        """Loaded customers should have all expected columns"""
        assert "id" in customers_df.columns
        assert "name" in customers_df.columns
        assert "email" in customers_df.columns
        assert "status" in customers_df.columns
        assert "country" in customers_df.columns
        assert "created_date" in customers_df.columns
        assert "updated_date" in customers_df.columns

    def test_customers_preserves_nulls(self, customers_df):
        """Bronze should NOT remove null values"""
        null_names = customers_df.filter(customers_df["name"].isNull()).count()
        assert null_names > 0

    def test_customers_preserves_mixed_case(self, customers_data):
        """Bronze should NOT normalise casing"""
        statuses = [row["status"] for row in customers_data]
        assert "ACTIVE" in statuses

    def test_customers_enforces_schema(self, customers_df):
        """Customers should be loaded with schema from YAML"""
        assert customers_df.schema == get_customers_schema()


# ===========================
# ORDERS SCHEMA TESTS
# ===========================


class TestOrdersSchema:

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
# ORDERS LOADING TESTS
# ===========================


class TestLoadOrders:

    def test_orders_loads_all_rows(self, orders_df):
        """Bronze should load all rows including dirty ones"""
        assert orders_df.count() == 8

    def test_orders_has_correct_columns(self, orders_df):
        """Loaded orders should have all expected columns"""
        assert "order_id" in orders_df.columns
        assert "customer_id" in orders_df.columns
        assert "product" in orders_df.columns
        assert "amount" in orders_df.columns
        assert "status" in orders_df.columns
        assert "order_date" in orders_df.columns

    def test_orders_preserves_nulls(self, orders_df):
        """Bronze should NOT remove null values"""
        null_dates = orders_df.filter(orders_df["order_date"].isNull()).count()
        assert null_dates > 0

    def test_orders_preserves_missing_customer_ids(self, orders_df):
        """Bronze should keep rows with missing customer IDs"""
        null_customers = orders_df.filter(orders_df["customer_id"].isNull()).count()
        assert null_customers > 0

    def test_orders_enforces_schema(self, orders_df):
        """Orders should be loaded with schema from YAML"""
        assert orders_df.schema == get_orders_schema()


# ===========================
# BRONZE TASK RUN TESTS
# ===========================


class TestRunBronze:

    @pytest.fixture(scope="class")
    def bronze_output(self, spark, tmp_path_factory):
        """Run bronze once and reuse output across all run tests"""
        output_path = str(tmp_path_factory.mktemp("bronze"))
        run_bronze(
            spark,
            customers_input="data/raw/customers.csv",
            orders_input="data/raw/orders.csv",
            output_path=output_path,
        )
        return output_path

    def test_run_bronze_creates_customers_output(self, bronze_output):
        """run_bronze should write customers parquet to output path"""
        assert os.path.exists(f"{bronze_output}/customers")

    def test_run_bronze_creates_orders_output(self, bronze_output):
        """run_bronze should write orders parquet to output path"""
        assert os.path.exists(f"{bronze_output}/orders")

    def test_run_bronze_customers_output_readable(self, spark, bronze_output):
        """Written customers parquet should be readable"""
        df = spark.read.parquet(f"{bronze_output}/customers")
        assert df.count() == 8

    def test_run_bronze_orders_output_readable(self, spark, bronze_output):
        """Written orders parquet should be readable"""
        df = spark.read.parquet(f"{bronze_output}/orders")
        assert df.count() == 8
