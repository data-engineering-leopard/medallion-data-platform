import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, TimestampType
)
from my_project.tasks.gold.dim_leads import (
    build_dim_leads,
    run_dim_leads
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("test_gold_dim_leads") \
        .getOrCreate()


@pytest.fixture
def silver_leads_schema():
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
        StructField("effective_from", TimestampType(), True),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), True)
    ])


class TestBuildDimLeads:

    def test_lead_key_is_added(self, spark, silver_leads_schema):
        """dim_leads should have a lead_key surrogate key column"""
        data = [(1, "John", "Smith", "john@acme.com", "Acme",
                 "open", "web", "UK", "2024-01-10", None, None, True)]
        df = spark.createDataFrame(data, silver_leads_schema)
        result = build_dim_leads(df)
        assert "lead_key" in result.columns

    def test_lead_key_is_unique(self, spark, silver_leads_schema):
        """Every row in dim_leads should have a unique lead_key"""
        data = [
            (1, "John", "Smith", "john@old.com", "Acme",
             "open", "web", "UK", "2024-01-10", None, None, False),
            (1, "John", "Smith", "john@new.com", "Acme",
             "open", "web", "UK", "2024-01-10", None, None, True)
        ]
        df = spark.createDataFrame(data, silver_leads_schema)
        result = build_dim_leads(df)
        total = result.count()
        distinct = result.select("lead_key").distinct().count()
        assert total == distinct

    def test_all_silver_columns_present(self, spark, silver_leads_schema):
        """All silver columns should be present in dim_leads"""
        data = [(1, "John", "Smith", "john@acme.com", "Acme",
                 "open", "web", "UK", "2024-01-10", None, None, True)]
        df = spark.createDataFrame(data, silver_leads_schema)
        result = build_dim_leads(df)
        assert "lead_id" in result.columns
        assert "first_name" in result.columns
        assert "last_name" in result.columns
        assert "email" in result.columns
        assert "company" in result.columns
        assert "status" in result.columns
        assert "lead_source" in result.columns
        assert "country" in result.columns
        assert "created_date" in result.columns
        assert "effective_from" in result.columns
        assert "effective_to" in result.columns
        assert "is_current" in result.columns

    def test_all_rows_preserved(self, spark, silver_leads_schema):
        """All rows including historical versions should be in dim_leads"""
        data = [
            (1, "John", "Smith", "john@old.com", "Acme",
             "open", "web", "UK", "2024-01-10", None, None, False),
            (1, "John", "Smith", "john@new.com", "Acme",
             "open", "web", "UK", "2024-01-10", None, None, True),
            (2, "Sarah", "Jones", "sarah@globex.com", "Globex",
             "working", "phone", "US", "2024-01-11", None, None, True)
        ]
        df = spark.createDataFrame(data, silver_leads_schema)
        result = build_dim_leads(df)
        assert result.count() == 3


class TestRunDimLeads:

    def test_run_dim_leads_creates_output(self, spark, tmp_path,
                                          silver_leads_schema):
        """run_dim_leads should write parquet to output path"""
        silver_path = str(tmp_path / "silver/leads")
        gold_path = str(tmp_path / "gold/dim_leads")

        data = [(1, "John", "Smith", "john@acme.com", "Acme",
                 "open", "web", "UK", "2024-01-10", None, None, True)]
        spark.createDataFrame(
            data, silver_leads_schema
        ).write.parquet(silver_path)

        run_dim_leads(
            spark,
            input_path=silver_path,
            output_path=gold_path
        )

        assert os.path.exists(gold_path)

    def test_run_dim_leads_output_readable(self, spark, tmp_path,
                                           silver_leads_schema):
        """Output parquet should be readable with correct row count"""
        silver_path = str(tmp_path / "silver/leads2")
        gold_path = str(tmp_path / "gold/dim_leads2")

        data = [(1, "John", "Smith", "john@acme.com", "Acme",
                 "open", "web", "UK", "2024-01-10", None, None, True)]
        spark.createDataFrame(
            data, silver_leads_schema
        ).write.parquet(silver_path)

        run_dim_leads(
            spark,
            input_path=silver_path,
            output_path=gold_path
        )

        result = spark.read.parquet(gold_path)
        assert result.count() == 1