import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from my_project.utils.scd2 import apply_scd2


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("test_scd2") \
        .getOrCreate()


@pytest.fixture
def customer_schema():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("status", StringType(), True),
        StructField("country", StringType(), True)
    ])


class TestScd2NewRecords:

    def test_new_records_are_inserted(self, spark, customer_schema):
        """New records should be inserted with is_current=True"""
        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK")
        ], customer_schema)

        existing = spark.createDataFrame([], customer_schema)

        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        assert result.count() == 1

    def test_new_records_have_is_current_true(self, spark, customer_schema):
        """New records should have is_current set to True"""
        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK")
        ], customer_schema)

        existing = spark.createDataFrame([], customer_schema)

        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        is_current = [row["is_current"] for row in result.collect()]
        assert all(is_current)

    def test_new_records_have_effective_to_as_max_date(
        self, spark, customer_schema
    ):
        """New records should have effective_to set to 9999-12-31"""
        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK")
        ], customer_schema)

        existing = spark.createDataFrame([], customer_schema)

        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        row = result.collect()[0]
        assert str(row["effective_to"]).startswith("9999")

    def test_new_records_have_effective_from_set(self, spark, customer_schema):
        """New records should have effective_from populated"""
        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK")
        ], customer_schema)

        existing = spark.createDataFrame([], customer_schema)

        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        row = result.collect()[0]
        assert row["effective_from"] is not None


class TestScd2ChangedRecords:

    def test_changed_record_creates_new_version(self, spark, customer_schema):
        """A changed tracked column should create a new version"""
        existing = spark.createDataFrame([
            (1, "ALICE", "alice@old.com", "active", "UK")
        ], customer_schema)

        existing = apply_scd2(
            spark=spark,
            incoming_df=existing,
            existing_df=spark.createDataFrame([], customer_schema),
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@new.com", "active", "UK")
        ], customer_schema)

        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        assert result.count() == 2

    def test_old_version_is_closed(self, spark, customer_schema):
        """The old version should have is_current set to False"""
        existing = spark.createDataFrame([
            (1, "ALICE", "alice@old.com", "active", "UK")
        ], customer_schema)

        existing = apply_scd2(
            spark=spark,
            incoming_df=existing,
            existing_df=spark.createDataFrame([], customer_schema),
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@new.com", "active", "UK")
        ], customer_schema)

        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        old_version = result.filter(
            result["email"] == "alice@old.com"
        ).collect()[0]
        assert old_version["is_current"] is False

    def test_new_version_is_current(self, spark, customer_schema):
        """The new version should have is_current set to True"""
        existing = spark.createDataFrame([
            (1, "ALICE", "alice@old.com", "active", "UK")
        ], customer_schema)

        existing = apply_scd2(
            spark=spark,
            incoming_df=existing,
            existing_df=spark.createDataFrame([], customer_schema),
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@new.com", "active", "UK")
        ], customer_schema)

        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        new_version = result.filter(
            result["email"] == "alice@new.com"
        ).collect()[0]
        assert new_version["is_current"] is True

    def test_old_version_effective_to_is_set(self, spark, customer_schema):
        """The old version should have effective_to set to a real date"""
        existing = spark.createDataFrame([
            (1, "ALICE", "alice@old.com", "active", "UK")
        ], customer_schema)

        existing = apply_scd2(
            spark=spark,
            incoming_df=existing,
            existing_df=spark.createDataFrame([], customer_schema),
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@new.com", "active", "UK")
        ], customer_schema)

        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        old_version = result.filter(
            result["email"] == "alice@old.com"
        ).collect()[0]
        assert not str(old_version["effective_to"]).startswith("9999")


class TestScd2UnchangedRecords:

    def test_unchanged_record_not_duplicated(self, spark, customer_schema):
        """A record with no changes to tracked columns should not be duplicated"""
        existing = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK")
        ], customer_schema)

        existing = apply_scd2(
            spark=spark,
            incoming_df=existing,
            existing_df=spark.createDataFrame([], customer_schema),
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK")
        ], customer_schema)

        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        assert result.count() == 1

    def test_unchanged_record_remains_current(self, spark, customer_schema):
        """An unchanged record should remain is_current=True"""
        existing = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK")
        ], customer_schema)

        existing = apply_scd2(
            spark=spark,
            incoming_df=existing,
            existing_df=spark.createDataFrame([], customer_schema),
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK")
        ], customer_schema)

        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        row = result.collect()[0]
        assert row["is_current"] is True


class TestScd2MultipleRecords:

    def test_multiple_customers_handled_independently(
        self, spark, customer_schema
    ):
        """Changes to one customer should not affect other customers"""
        existing = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK"),
            (2, "BOB", "bob@email.com", "active", "US")
        ], customer_schema)

        existing = apply_scd2(
            spark=spark,
            incoming_df=existing,
            existing_df=spark.createDataFrame([], customer_schema),
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@new.com", "active", "UK"),
            (2, "BOB", "bob@email.com", "active", "US")
        ], customer_schema)

        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        alice_count = result.filter(result["id"] == 1).count()
        bob_count = result.filter(result["id"] == 2).count()

        assert alice_count == 2
        assert bob_count == 1