import os
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)
from my_project.tasks.silver.silver_task import (
    load_silver_config,
    apply_cleaning_rules,
    run_silver_table,
    run_silver,
)

# ===========================
# CONFIG LOADING TESTS
# ===========================


class TestLoadSilverConfig:

    def test_loads_all_table_configs(self):
        """Should load all YAML files from the silver config folder"""
        configs = load_silver_config("assets/silver")
        table_names = [c["table"] for c in configs]
        assert "customers" in table_names
        assert "orders" in table_names

    def test_config_has_required_fields(self):
        """Each config should have all required fields"""
        configs = load_silver_config("assets/silver")
        for config in configs:
            assert "table" in config
            assert "input_path" in config
            assert "output_path" in config
            assert "scd2" in config

    def test_customers_config_is_scd2(self):
        """Customers table should be marked as scd2"""
        configs = load_silver_config("assets/silver")
        customers = next(c for c in configs if c["table"] == "customers")
        assert customers["scd2"] is True

    def test_orders_config_is_not_scd2(self):
        """Orders table should not be marked as scd2"""
        configs = load_silver_config("assets/silver")
        orders = next(c for c in configs if c["table"] == "orders")
        assert orders["scd2"] is False


# ===========================
# CLEANING RULES TESTS
# ===========================


class TestApplyCleaningRules:

    def test_drops_null_columns(self, spark, customer_schema):
        """Rows with nulls in specified columns should be removed"""
        data = [
            (1, "ALICE", "alice@email.com", "active", "UK"),
            (2, None, "ghost@email.com", "active", "US"),
        ]
        df = spark.createDataFrame(data, customer_schema)
        config = {
            "drop_null_columns": ["name"],
            "uppercase_columns": [],
            "lowercase_columns": [],
        }
        result = apply_cleaning_rules(df, config)
        assert result.count() == 1

    def test_uppercases_columns(self, spark, customer_schema):
        """Specified columns should be uppercased"""
        data = [(1, "alice", "alice@email.com", "active", "UK")]
        df = spark.createDataFrame(data, customer_schema)
        config = {
            "drop_null_columns": [],
            "uppercase_columns": ["name"],
            "lowercase_columns": [],
        }
        result = apply_cleaning_rules(df, config)
        names = [row["name"] for row in result.collect()]
        assert all(n == n.upper() for n in names)

    def test_lowercases_columns(self, spark, customer_schema):
        """Specified columns should be lowercased"""
        data = [(1, "ALICE", "alice@email.com", "ACTIVE", "UK")]
        df = spark.createDataFrame(data, customer_schema)
        config = {
            "drop_null_columns": [],
            "uppercase_columns": [],
            "lowercase_columns": ["status"],
        }
        result = apply_cleaning_rules(df, config)
        statuses = [row["status"] for row in result.collect()]
        assert all(s == s.lower() for s in statuses)

    def test_all_rules_applied_together(self, spark, customer_schema):
        """All cleaning rules should be applied in combination"""
        data = [
            (1, "alice", "alice@email.com", "ACTIVE", "UK"),
            (2, None, "ghost@email.com", "active", "US"),
        ]
        df = spark.createDataFrame(data, customer_schema)
        config = {
            "drop_null_columns": ["name"],
            "uppercase_columns": ["name"],
            "lowercase_columns": ["status"],
        }
        result = apply_cleaning_rules(df, config)
        assert result.count() == 1
        row = result.collect()[0]
        assert row["name"] == "ALICE"
        assert row["status"] == "active"


# ===========================
# SILVER TABLE RUN TESTS
# ===========================


class TestRunSilverTable:

    def test_run_silver_table_creates_output(self, spark, tmp_path):
        """run_silver_table should write output parquet"""
        bronze_path = str(tmp_path / "bronze/customers")
        silver_path = str(tmp_path / "silver/customers")

        data = [(1, "alice", "alice@email.com", "ACTIVE", "UK")]
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("status", StringType(), True),
                StructField("country", StringType(), True),
            ]
        )
        spark.createDataFrame(data, schema).write.parquet(bronze_path)

        config = {
            "table": "customers",
            "input_path": bronze_path,
            "output_path": silver_path,
            "scd2": False,
            "drop_null_columns": ["name"],
            "uppercase_columns": ["name"],
            "lowercase_columns": ["status"],
        }

        run_silver_table(spark, config)
        assert os.path.exists(silver_path)

    def test_run_silver_table_output_is_readable(self, spark, tmp_path):
        """Output parquet should be readable after run_silver_table"""
        bronze_path = str(tmp_path / "bronze/customers2")
        silver_path = str(tmp_path / "silver/customers2")

        data = [(1, "alice", "alice@email.com", "ACTIVE", "UK")]
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("status", StringType(), True),
                StructField("country", StringType(), True),
            ]
        )
        spark.createDataFrame(data, schema).write.parquet(bronze_path)

        config = {
            "table": "customers",
            "input_path": bronze_path,
            "output_path": silver_path,
            "scd2": False,
            "drop_null_columns": ["name"],
            "uppercase_columns": ["name"],
            "lowercase_columns": ["status"],
        }

        run_silver_table(spark, config)
        result = spark.read.parquet(silver_path)
        assert result.count() == 1


# ===========================
# FULL SILVER RUN TESTS
# ===========================


class TestRunSilver:

    def test_run_silver_processes_all_tables(self, spark, tmp_path):
        """run_silver should process every table in the config folder"""
        customer_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("status", StringType(), True),
                StructField("country", StringType(), True),
                StructField("created_date", StringType(), True),
                StructField("updated_date", StringType(), True),
            ]
        )
        order_schema = StructType(
            [
                StructField("order_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("product", StringType(), True),
                StructField("amount", FloatType(), True),
                StructField("status", StringType(), True),
                StructField("order_date", StringType(), True),
            ]
        )
        leads_schema = StructType(
            [
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
            ]
        )

        spark.createDataFrame(
            [(1, "alice", "alice@email.com", "active", "UK", "2024-01-01", None)],
            customer_schema,
        ).write.parquet(str(tmp_path / "bronze/customers"))

        spark.createDataFrame(
            [(1, 1, "Laptop", 999.99, "completed", "2024-01-15")], order_schema
        ).write.parquet(str(tmp_path / "bronze/orders"))

        spark.createDataFrame(
            [
                (
                    1,
                    "John",
                    "Smith",
                    "john@acme.com",
                    "Acme Corp",
                    "open",
                    "web",
                    "UK",
                    "2024-01-10",
                    None,
                )
            ],
            leads_schema,
        ).write.parquet(str(tmp_path / "bronze/leads"))

        run_silver(
            spark,
            config_path="assets/silver",
            bronze_base_path=str(tmp_path / "bronze"),
            silver_base_path=str(tmp_path / "silver"),
        )

        assert os.path.exists(str(tmp_path / "silver/customers"))
        assert os.path.exists(str(tmp_path / "silver/orders"))
        assert os.path.exists(str(tmp_path / "silver/leads"))


class TestSilverQuarantine:

    def test_quarantine_written_for_records_missing_dates(self, spark, tmp_path):
        """Records missing both date columns should be written to quarantine"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        bronze_path = str(tmp_path / "bronze/customers_q")
        silver_path = str(tmp_path / "silver/customers_q")
        quarantine_path = str(tmp_path / "quarantine/customers_q")

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("status", StringType(), True),
                StructField("country", StringType(), True),
                StructField("created_date", StringType(), True),
                StructField("updated_date", StringType(), True),
            ]
        )

        spark.createDataFrame(
            [
                (1, "ALICE", "alice@email.com", "active", "UK", "2024-01-01", None),
                (2, "BOB", "bob@email.com", "active", "US", None, None),
            ],
            schema,
        ).write.parquet(bronze_path)

        config = {
            "table": "customers",
            "input_path": bronze_path,
            "output_path": silver_path,
            "quarantine_path": quarantine_path,
            "scd2": True,
            "scd2_key": "id",
            "effective_from_column": "updated_date",
            "effective_from_fallback_column": "created_date",
            "scd2_track_columns": ["email", "status", "country"],
            "drop_null_columns": [],
            "uppercase_columns": [],
            "lowercase_columns": [],
        }

        run_silver_table(spark, config)

        assert os.path.exists(quarantine_path)
        quarantine_df = spark.read.parquet(quarantine_path)
        assert quarantine_df.count() == 1

    def test_valid_records_not_in_quarantine(self, spark, tmp_path):
        """Valid records should not appear in quarantine"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        bronze_path = str(tmp_path / "bronze/customers_v")
        silver_path = str(tmp_path / "silver/customers_v")
        quarantine_path = str(tmp_path / "quarantine/customers_v")

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("status", StringType(), True),
                StructField("country", StringType(), True),
                StructField("created_date", StringType(), True),
                StructField("updated_date", StringType(), True),
            ]
        )

        spark.createDataFrame(
            [(1, "ALICE", "alice@email.com", "active", "UK", "2024-01-01", None)],
            schema,
        ).write.parquet(bronze_path)

        config = {
            "table": "customers",
            "input_path": bronze_path,
            "output_path": silver_path,
            "quarantine_path": quarantine_path,
            "scd2": True,
            "scd2_key": "id",
            "effective_from_column": "updated_date",
            "effective_from_fallback_column": "created_date",
            "scd2_track_columns": ["email", "status", "country"],
            "drop_null_columns": [],
            "uppercase_columns": [],
            "lowercase_columns": [],
        }

        run_silver_table(spark, config)

        if os.path.exists(quarantine_path):
            quarantine_df = spark.read.parquet(quarantine_path)
            assert quarantine_df.count() == 0
