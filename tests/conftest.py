import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, LongType, BooleanType, TimestampType
)


# ===========================
# SPARK SESSION
# ===========================

@pytest.fixture(scope="session")
def spark():
    """
    Single SparkSession shared across the entire test suite.
    scope="session" means this is created once and reused
    for every test in every test file.
    """
    return SparkSession.builder \
        .master("local[*]") \
        .appName("test_suite") \
        .getOrCreate()


# ===========================
# BRONZE SCHEMAS
# ===========================

@pytest.fixture(scope="session")
def raw_schema():
    """Raw customer schema matching the CSV structure"""
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("status", StringType(), True),
        StructField("country", StringType(), True)
    ])


@pytest.fixture(scope="session")
def customer_schema():
    """Basic customer schema without date columns"""
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("status", StringType(), True),
        StructField("country", StringType(), True)
    ])


@pytest.fixture(scope="session")
def customer_schema_with_dates():
    """Customer schema with created_date and updated_date columns"""
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("status", StringType(), True),
        StructField("country", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("updated_date", StringType(), True)
    ])


@pytest.fixture(scope="session")
def order_schema():
    """Basic order schema"""
    return StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("status", StringType(), True),
        StructField("order_date", StringType(), True)
    ])


# ===========================
# SILVER SCHEMAS
# ===========================

@pytest.fixture(scope="session")
def silver_customers_schema():
    """Silver customers schema including SCD2 columns"""
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("status", StringType(), True),
        StructField("country", StringType(), True),
        StructField("effective_from", TimestampType(), True),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), True)
    ])


@pytest.fixture(scope="session")
def silver_orders_schema():
    """Silver orders schema"""
    return StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("status", StringType(), True),
        StructField("order_date", StringType(), True)
    ])


@pytest.fixture(scope="session")
def silver_leads_schema():
    """Silver leads schema including SCD2 columns"""
    return StructType([
        StructField("lead_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("company", StringType(), True),
        StructField("status", StringType(), True),
        StructField("lead_source", StringType(), True),
        StructField("country", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("updated_date", StringType(), True),
        StructField("effective_from", TimestampType(), True),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), True)
    ])


# ===========================
# GOLD SCHEMAS
# ===========================

@pytest.fixture(scope="session")
def dim_customers_schema():
    """Gold dim_customers schema including surrogate key and SCD2 columns"""
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