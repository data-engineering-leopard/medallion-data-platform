import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from my_project.layers.bronze import load_bronze
from my_project.layers.silver import transform_silver
from my_project.layers.gold import transform_gold

@pytest.fixture(scope="session")
def raw_schema():
    """Schema matching our customers CSV"""
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("status", StringType(), True),
        StructField("country", StringType(), True)
    ])


# ===========================
# BRONZE TESTS
# ===========================

class TestBronzeLayer:

    def test_bronze_loads_all_rows(self, spark):
        """Bronze should load every row including dirty ones"""
        bronze_df = load_bronze(spark, "data/raw/customers.csv")
        assert bronze_df.count() == 8

    def test_bronze_has_correct_columns(self, spark):
        """Bronze should have all the original columns"""
        bronze_df = load_bronze(spark, "data/raw/customers.csv")
        assert "id" in bronze_df.columns
        assert "name" in bronze_df.columns
        assert "email" in bronze_df.columns
        assert "status" in bronze_df.columns
        assert "country" in bronze_df.columns

    def test_bronze_preserves_raw_data(self, spark):
        """Bronze should NOT clean anything - mixed case should still be there"""
        bronze_df = load_bronze(spark, "data/raw/customers.csv")
        statuses = [row["status"] for row in bronze_df.collect()]
        assert "ACTIVE" in statuses


# ===========================
# SILVER TESTS
# ===========================

class TestSilverLayer:

    def test_silver_removes_null_names(self, spark, raw_schema):
        """Rows with null names should be removed"""
        data = [
            (1, "Alice", "alice@email.com", "active", "UK"),
            (2, None, "ghost@email.com", "active", "US")
        ]
        df = spark.createDataFrame(data, raw_schema)
        result = transform_silver(df)
        assert result.count() == 1

    def test_silver_removes_null_countries(self, spark, raw_schema):
        """Rows with null countries should be removed"""
        data = [
            (1, "Alice", "alice@email.com", "active", "UK"),
            (2, "Eve", "eve@email.com", "active", None)
        ]
        df = spark.createDataFrame(data, raw_schema)
        result = transform_silver(df)
        assert result.count() == 1

    def test_silver_uppercases_names(self, spark, raw_schema):
        """All names should be uppercased"""
        data = [
            (1, "alice", "alice@email.com", "active", "UK"),
            (2, "Bob", "bob@email.com", "active", "US")
        ]
        df = spark.createDataFrame(data, raw_schema)
        result = transform_silver(df)
        names = [row["name"] for row in result.collect()]
        assert all(name == name.upper() for name in names)

    def test_silver_lowercases_status(self, spark, raw_schema):
        """All status values should be lowercased"""
        data = [
            (1, "Alice", "alice@email.com", "ACTIVE", "UK"),
            (2, "Bob", "bob@email.com", "Active", "US")
        ]
        df = spark.createDataFrame(data, raw_schema)
        result = transform_silver(df)
        statuses = [row["status"] for row in result.collect()]
        assert all(status == status.lower() for status in statuses)

    def test_silver_keeps_valid_rows(self, spark, raw_schema):
        """Rows with all required fields should be kept"""
        data = [
            (1, "Alice", "alice@email.com", "active", "UK"),
            (2, "Bob", "bob@email.com", "active", "US")
        ]
        df = spark.createDataFrame(data, raw_schema)
        result = transform_silver(df)
        assert result.count() == 2


# ===========================
# GOLD TESTS
# ===========================

class TestGoldLayer:

    def test_gold_counts_active_customers_by_country(self, spark, raw_schema):
        """Should return correct active customer count per country"""
        data = [
            (1, "ALICE", "alice@email.com", "active", "UK"),
            (2, "BOB", "bob@email.com", "active", "UK"),
            (3, "CHARLIE", "charlie@email.com", "active", "US"),
            (4, "DIANA", "diana@email.com", "inactive", "UK")
        ]
        df = spark.createDataFrame(data, raw_schema)
        result = transform_gold(df)

        uk_row = result.filter(result["country"] == "UK").collect()
        assert uk_row[0]["active_customer_count"] == 2

    def test_gold_excludes_inactive_customers(self, spark, raw_schema):
        """Inactive customers should not appear in gold output"""
        data = [
            (1, "ALICE", "alice@email.com", "inactive", "UK"),
            (2, "BOB", "bob@email.com", "inactive", "US")
        ]
        df = spark.createDataFrame(data, raw_schema)
        result = transform_gold(df)
        assert result.count() == 0

    def test_gold_groups_by_country(self, spark, raw_schema):
        """Each country should appear only once in gold output"""
        data = [
            (1, "ALICE", "alice@email.com", "active", "UK"),
            (2, "BOB", "bob@email.com", "active", "UK"),
            (3, "CHARLIE", "charlie@email.com", "active", "US")
        ]
        df = spark.createDataFrame(data, raw_schema)
        result = transform_gold(df)
        assert result.count() == 2

    def test_gold_output_has_correct_columns(self, spark, raw_schema):
        """Gold output should only have country and active_customer_count"""
        data = [
            (1, "ALICE", "alice@email.com", "active", "UK")
        ]
        df = spark.createDataFrame(data, raw_schema)
        result = transform_gold(df)
        assert "country" in result.columns
        assert "active_customer_count" in result.columns