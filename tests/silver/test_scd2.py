import pytest
from my_project.utils.scd2 import (
    apply_scd2,
    separate_quarantine_records,
    resolve_effective_from,
    merge_scd2
)

# ===========================
# SESSION FIXTURES
# ===========================

@pytest.fixture(scope="session")
def empty_customer_df(spark, customer_schema):
    """Reusable empty customer DataFrame"""
    return spark.createDataFrame([], customer_schema)


@pytest.fixture(scope="session")
def empty_customer_with_dates_df(spark, customer_schema_with_dates):
    """Reusable empty customer DataFrame with date columns"""
    return spark.createDataFrame([], customer_schema_with_dates)


@pytest.fixture(scope="session")
def single_customer_df(spark, customer_schema):
    """Single active customer — reused across multiple tests"""
    return spark.createDataFrame([
        (1, "ALICE", "alice@email.com", "active", "UK")
    ], customer_schema)


@pytest.fixture(scope="session")
def two_customer_df(spark, customer_schema):
    """Two active customers — reused across multiple tests"""
    return spark.createDataFrame([
        (1, "ALICE", "alice@email.com", "active", "UK"),
        (2, "BOB", "bob@email.com", "active", "US")
    ], customer_schema)


@pytest.fixture(scope="session")
def existing_single_customer(spark, customer_schema, empty_customer_df):
    """
    Pre-built SCD2 state with one customer already processed.
    Reused across changed/unchanged record tests.
    """
    incoming = spark.createDataFrame([
        (1, "ALICE", "alice@old.com", "active", "UK")
    ], customer_schema)
    return apply_scd2(
        spark=spark,
        incoming_df=incoming,
        existing_df=empty_customer_df,
        scd2_key="id",
        track_columns=["email", "status", "country"]
    )["valid"]


@pytest.fixture(scope="session")
def existing_two_customers(spark, customer_schema, empty_customer_df):
    """
    Pre-built SCD2 state with two customers already processed.
    Reused across multiple record tests.
    """
    incoming = spark.createDataFrame([
        (1, "ALICE", "alice@email.com", "active", "UK"),
        (2, "BOB", "bob@email.com", "active", "US")
    ], customer_schema)
    return apply_scd2(
        spark=spark,
        incoming_df=incoming,
        existing_df=empty_customer_df,
        scd2_key="id",
        track_columns=["email", "status", "country"]
    )["valid"]

@pytest.fixture(scope="session")
def changed_record_result(spark, customer_schema, existing_single_customer):
    """Run SCD2 once with a changed email"""
    incoming = spark.createDataFrame([
        (1, "ALICE", "alice@new.com", "active", "UK")
    ], customer_schema)
    return apply_scd2(
        spark=spark,
        incoming_df=incoming,
        existing_df=existing_single_customer,
        scd2_key="id",
        track_columns=["email", "status", "country"]
    )["valid"].cache()


@pytest.fixture(scope="session")
def unchanged_record_result(spark, customer_schema, existing_single_customer):
    """Run SCD2 once with same data — nothing should change"""
    incoming = spark.createDataFrame([
        (1, "ALICE", "alice@old.com", "active", "UK")
    ], customer_schema)
    return apply_scd2(
        spark=spark,
        incoming_df=incoming,
        existing_df=existing_single_customer,
        scd2_key="id",
        track_columns=["email", "status", "country"]
    )["valid"].cache()


@pytest.fixture(scope="session")
def multiple_record_result(spark, customer_schema, existing_two_customers):
    """Run SCD2 once with Alice changed and Bob unchanged"""
    incoming = spark.createDataFrame([
        (1, "ALICE", "alice@new.com", "active", "UK"),
        (2, "BOB", "bob@email.com", "active", "US")
    ], customer_schema)
    return apply_scd2(
        spark=spark,
        incoming_df=incoming,
        existing_df=existing_two_customers,
        scd2_key="id",
        track_columns=["email", "status", "country"]
    )["valid"].cache()

# ===========================
# UNIT TESTS — separate_quarantine_records
# ===========================

class TestSeparateQuarantineRecords:

    def test_valid_records_returned_when_updated_date_present(
        self, spark, customer_schema_with_dates
    ):
        """Records with updated_date should be in valid"""
        df = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK",
             "2024-01-01", "2024-03-15")
        ], customer_schema_with_dates)

        result = separate_quarantine_records(
            df,
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )

        assert result["valid"].count() == 1
        assert result["quarantine"].count() == 0

    def test_valid_records_returned_when_only_created_date_present(
        self, spark, customer_schema_with_dates
    ):
        """Records with only created_date should be in valid"""
        df = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK",
             "2024-01-01", None)
        ], customer_schema_with_dates)

        result = separate_quarantine_records(
            df,
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )

        assert result["valid"].count() == 1
        assert result["quarantine"].count() == 0

    def test_records_quarantined_when_both_dates_missing(
        self, spark, customer_schema_with_dates
    ):
        """Records missing both dates should be quarantined"""
        df = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK", None, None)
        ], customer_schema_with_dates)

        result = separate_quarantine_records(
            df,
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )

        assert result["valid"].count() == 0
        assert result["quarantine"].count() == 1

    def test_mixed_records_split_correctly(
        self, spark, customer_schema_with_dates
    ):
        """Valid and quarantine records should be split correctly"""
        df = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK",
             "2024-01-01", None),
            (2, "BOB", "bob@email.com", "active", "US", None, None)
        ], customer_schema_with_dates)

        result = separate_quarantine_records(
            df,
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )

        assert result["valid"].count() == 1
        assert result["quarantine"].count() == 1

    def test_returns_all_records_when_no_date_columns(
        self, spark, customer_schema
    ):
        """When no date columns specified all records should be valid"""
        df = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK"),
            (2, "BOB", "bob@email.com", "active", "US")
        ], customer_schema)

        result = separate_quarantine_records(
            df,
            effective_from_column=None,
            effective_from_fallback_column=None
        )

        assert result["valid"].count() == 2
        assert result["quarantine"].count() == 0


# ===========================
# UNIT TESTS — resolve_effective_from
# ===========================

class TestResolveEffectiveFrom:

    def test_uses_updated_date_when_present(
        self, spark, customer_schema_with_dates
    ):
        """Should use updated_date as effective_from when present"""
        df = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK",
             "2024-01-01", "2024-03-15")
        ], customer_schema_with_dates)

        result = resolve_effective_from(
            df,
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )

        row = result.collect()[0]
        assert str(row["effective_from"]).startswith("2024-03-15")

    def test_falls_back_to_created_date_when_updated_date_null(
        self, spark, customer_schema_with_dates
    ):
        """Should fall back to created_date when updated_date is null"""
        df = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK",
             "2024-01-01", None)
        ], customer_schema_with_dates)

        result = resolve_effective_from(
            df,
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )

        row = result.collect()[0]
        assert str(row["effective_from"]).startswith("2024-01-01")

    def test_uses_hardcoded_ts_when_no_date_columns(
        self, spark, customer_schema
    ):
        """Should use hardcoded timestamp when no date columns specified"""
        df = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK")
        ], customer_schema)

        result = resolve_effective_from(
            df,
            effective_from_column=None,
            effective_from_fallback_column=None,
            effective_from_ts="2024-01-01 00:00:00"
        )

        row = result.collect()[0]
        assert str(row["effective_from"]).startswith("2024-01-01")

    def test_adds_effective_from_column(
        self, spark, customer_schema_with_dates
    ):
        """Should add effective_from column to DataFrame"""
        df = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK",
             "2024-01-01", None)
        ], customer_schema_with_dates)

        result = resolve_effective_from(
            df,
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )

        assert "effective_from" in result.columns

    def test_adds_effective_to_column(
        self, spark, customer_schema_with_dates
    ):
        """Should add effective_to column set to max date"""
        df = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK",
             "2024-01-01", None)
        ], customer_schema_with_dates)

        result = resolve_effective_from(
            df,
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )

        row = result.collect()[0]
        assert str(row["effective_to"]).startswith("9999")

    def test_adds_is_current_column(
        self, spark, customer_schema_with_dates
    ):
        """Should add is_current column set to True"""
        df = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK",
             "2024-01-01", None)
        ], customer_schema_with_dates)

        result = resolve_effective_from(
            df,
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )

        row = result.collect()[0]
        assert row["is_current"] is True


# ===========================
# UNIT TESTS — merge_scd2
# ===========================

class TestMergeScd2:

    @pytest.fixture(scope="class")
    def scd2_schema(self, customer_schema):
        """Customer schema with SCD2 metadata columns added"""
        from pyspark.sql.types import TimestampType, BooleanType
        return StructType(
            customer_schema.fields + [
                StructField("effective_from", TimestampType(), True),
                StructField("effective_to", TimestampType(), True),
                StructField("is_current", BooleanType(), True)
            ]
        )

    def test_new_records_inserted_when_no_existing(
        self, spark, customer_schema, empty_customer_df
    ):
        """New records should be inserted when no existing data"""
        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK")
        ], customer_schema)

        incoming_with_meta = resolve_effective_from(
            incoming,
            effective_from_column=None,
            effective_from_fallback_column=None,
            effective_from_ts="2024-01-01 00:00:00"
        )

        result = merge_scd2(
            incoming_df=incoming_with_meta,
            existing_df=empty_customer_df,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        assert result.count() == 1

    def test_changed_record_creates_two_versions(
        self, spark, customer_schema, empty_customer_df
    ):
        """A changed record should result in two versions"""
        initial = spark.createDataFrame([
            (1, "ALICE", "alice@old.com", "active", "UK")
        ], customer_schema)

        existing = resolve_effective_from(
            initial,
            effective_from_column=None,
            effective_from_fallback_column=None,
            effective_from_ts="2024-01-01 00:00:00"
        )

        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@new.com", "active", "UK")
        ], customer_schema)

        incoming_with_meta = resolve_effective_from(
            incoming,
            effective_from_column=None,
            effective_from_fallback_column=None,
            effective_from_ts="2024-03-15 00:00:00"
        )

        result = merge_scd2(
            incoming_df=incoming_with_meta,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        assert result.count() == 2

    def test_unchanged_record_not_duplicated(
        self, spark, customer_schema, empty_customer_df
    ):
        """An unchanged record should not be duplicated"""
        initial = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK")
        ], customer_schema)

        existing = resolve_effective_from(
            initial,
            effective_from_column=None,
            effective_from_fallback_column=None,
            effective_from_ts="2024-01-01 00:00:00"
        )

        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK")
        ], customer_schema)

        incoming_with_meta = resolve_effective_from(
            incoming,
            effective_from_column=None,
            effective_from_fallback_column=None,
            effective_from_ts="2024-01-01 00:00:00"
        )

        result = merge_scd2(
            incoming_df=incoming_with_meta,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        assert result.count() == 1

    def test_old_version_closed_on_change(
        self, spark, customer_schema, empty_customer_df
    ):
        """Old version should have is_current=False after a change"""
        initial = spark.createDataFrame([
            (1, "ALICE", "alice@old.com", "active", "UK")
        ], customer_schema)

        existing = resolve_effective_from(
            initial,
            effective_from_column=None,
            effective_from_fallback_column=None,
            effective_from_ts="2024-01-01 00:00:00"
        )

        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@new.com", "active", "UK")
        ], customer_schema)

        incoming_with_meta = resolve_effective_from(
            incoming,
            effective_from_column=None,
            effective_from_fallback_column=None,
            effective_from_ts="2024-03-15 00:00:00"
        )

        result = merge_scd2(
            incoming_df=incoming_with_meta,
            existing_df=existing,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )

        old = result.filter(result["email"] == "alice@old.com").collect()[0]
        assert old["is_current"] is False

# ===========================
# NEW RECORDS TESTS
# ===========================

class TestScd2NewRecords:

    @pytest.fixture(scope="class")
    def new_record_result(self, spark, single_customer_df, empty_customer_df):
        """Run SCD2 once for all new record tests"""
        return apply_scd2(
            spark=spark,
            incoming_df=single_customer_df,
            existing_df=empty_customer_df,
            scd2_key="id",
            track_columns=["email", "status", "country"]
        )["valid"].cache()

    def test_new_records_are_inserted(self, new_record_result):
        """New records should be inserted"""
        assert new_record_result.count() == 1

    def test_new_records_have_is_current_true(self, new_record_result):
        """New records should have is_current set to True"""
        row = new_record_result.collect()[0]
        assert row["is_current"] is True

    def test_new_records_have_effective_to_as_max_date(
        self, new_record_result
    ):
        """New records should have effective_to set to 9999-12-31"""
        row = new_record_result.collect()[0]
        assert str(row["effective_to"]).startswith("9999")

    def test_new_records_have_effective_from_set(self, new_record_result):
        """New records should have effective_from populated"""
        row = new_record_result.collect()[0]
        assert row["effective_from"] is not None


# ===========================
# CHANGED RECORDS TESTS
# ===========================

class TestScd2ChangedRecords:

    def test_changed_record_creates_new_version(
        self, changed_record_result
    ):
        """A changed tracked column should create a new version"""
        assert changed_record_result.count() == 2

    def test_old_version_is_closed(self, changed_record_result):
        """The old version should have is_current set to False"""
        old = changed_record_result.filter(
            changed_record_result["email"] == "alice@old.com"
        ).collect()[0]
        assert old["is_current"] is False

    def test_new_version_is_current(self, changed_record_result):
        """The new version should have is_current set to True"""
        new = changed_record_result.filter(
            changed_record_result["email"] == "alice@new.com"
        ).collect()[0]
        assert new["is_current"] is True

    def test_old_version_effective_to_is_set(self, changed_record_result):
        """The old version should have effective_to set to a real date"""
        old = changed_record_result.filter(
            changed_record_result["email"] == "alice@old.com"
        ).collect()[0]
        assert not str(old["effective_to"]).startswith("9999")


# ===========================
# UNCHANGED RECORDS TESTS
# ===========================

class TestScd2UnchangedRecords:

    def test_unchanged_record_not_duplicated(
        self, unchanged_record_result
    ):
        """A record with no changes should not be duplicated"""
        assert unchanged_record_result.count() == 1

    def test_unchanged_record_remains_current(
        self, unchanged_record_result
    ):
        """An unchanged record should remain is_current=True"""
        row = unchanged_record_result.collect()[0]
        assert row["is_current"] is True


# ===========================
# MULTIPLE RECORDS TESTS
# ===========================

class TestScd2MultipleRecords:

    def test_multiple_customers_handled_independently(
        self, multiple_record_result
    ):
        """Changes to one customer should not affect other customers"""
        alice_count = multiple_record_result.filter(
            multiple_record_result["id"] == 1
        ).count()
        bob_count = multiple_record_result.filter(
            multiple_record_result["id"] == 2
        ).count()
        assert alice_count == 2
        assert bob_count == 1


# ===========================
# EVENT DATE TESTS
# ===========================

class TestScd2EventDate:

    @pytest.fixture(scope="class")
    def updated_date_result(
        self, spark, customer_schema_with_dates,
        empty_customer_with_dates_df
    ):
        """Run SCD2 once with updated_date for event date tests"""
        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK",
             "2024-01-01", "2024-03-15")
        ], customer_schema_with_dates)
        return apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=empty_customer_with_dates_df,
            scd2_key="id",
            track_columns=["email", "status", "country"],
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )["valid"].cache()

    @pytest.fixture(scope="class")
    def created_date_fallback_result(
        self, spark, customer_schema_with_dates,
        empty_customer_with_dates_df
    ):
        """Run SCD2 once with null updated_date for fallback tests"""
        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK",
             "2024-01-01", None)
        ], customer_schema_with_dates)
        return apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=empty_customer_with_dates_df,
            scd2_key="id",
            track_columns=["email", "status", "country"],
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )["valid"].cache()

    def test_uses_updated_date_as_effective_from(
        self, updated_date_result
    ):
        """effective_from should be set from updated_date when provided"""
        row = updated_date_result.collect()[0]
        assert str(row["effective_from"]).startswith("2024-03-15")

    def test_falls_back_to_created_date_when_no_updated_date(
        self, created_date_fallback_result
    ):
        """effective_from should fall back to created_date when updated_date is null"""
        row = created_date_fallback_result.collect()[0]
        assert str(row["effective_from"]).startswith("2024-01-01")


# ===========================
# QUARANTINE TESTS
# ===========================

class TestScd2Quarantine:

    @pytest.fixture(scope="class")
    def mixed_quarantine_result(
        self, spark, customer_schema_with_dates,
        empty_customer_with_dates_df
    ):
        """
        Run SCD2 once with one valid and one quarantine record.
        Alice has created_date — valid.
        Bob has neither date — quarantined.
        """
        incoming = spark.createDataFrame([
            (1, "ALICE", "alice@email.com", "active", "UK",
             "2024-01-01", None),
            (2, "BOB", "bob@email.com", "active", "US", None, None)
        ], customer_schema_with_dates)
        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=empty_customer_with_dates_df,
            scd2_key="id",
            track_columns=["email", "status", "country"],
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )
        return {
            "valid": result["valid"].cache(),
            "quarantine": result["quarantine"].cache()
        }

    @pytest.fixture(scope="class")
    def all_quarantine_result(
        self, spark, customer_schema_with_dates,
        empty_customer_with_dates_df
    ):
        """Run SCD2 once with only quarantine records"""
        incoming = spark.createDataFrame([
            (2, "BOB", "bob@email.com", "active", "US", None, None)
        ], customer_schema_with_dates)
        result = apply_scd2(
            spark=spark,
            incoming_df=incoming,
            existing_df=empty_customer_with_dates_df,
            scd2_key="id",
            track_columns=["email", "status", "country"],
            effective_from_column="updated_date",
            effective_from_fallback_column="created_date"
        )
        return {
            "valid": result["valid"].cache(),
            "quarantine": result["quarantine"].cache()
        }

    def test_records_missing_both_dates_are_quarantined(
        self, mixed_quarantine_result
    ):
        """Records with no dates should be quarantined"""
        assert mixed_quarantine_result["valid"].count() == 1
        assert mixed_quarantine_result["quarantine"].count() == 1

    def test_quarantined_records_not_in_silver(
        self, all_quarantine_result
    ):
        """Quarantined records should not appear in valid output"""
        assert all_quarantine_result["valid"].count() == 0

    def test_quarantined_records_captured_separately(
        self, mixed_quarantine_result
    ):
        """Quarantined records should be returned separately"""
        assert mixed_quarantine_result["quarantine"].count() == 1