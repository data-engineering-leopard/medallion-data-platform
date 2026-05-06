import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from my_project.tasks.bronze.bronze_salesforce import (
    load_leads,
    get_leads_schema,
    run_bronze_salesforce
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("test_bronze_salesforce") \
        .getOrCreate()


# ===========================
# SCHEMA TESTS
# ===========================

class TestSchemas:

    def test_leads_schema_loaded_from_yaml(self):
        """Leads schema should be loaded from YAML not hardcoded"""
        schema = get_leads_schema()
        assert isinstance(schema, StructType)

    def test_leads_schema_has_correct_columns(self):
        """Leads schema should define all expected columns"""
        schema = get_leads_schema()
        field_names = [field.name for field in schema.fields]
        assert "lead_id" in field_names
        assert "first_name" in field_names
        assert "last_name" in field_names
        assert "email" in field_names
        assert "company" in field_names
        assert "status" in field_names
        assert "lead_source" in field_names
        assert "country" in field_names
        assert "created_date" in field_names


# ===========================
# LEADS LOADING TESTS
# ===========================

class TestLoadLeads:

    def test_leads_loads_all_rows(self, spark):
        """Bronze should load all rows including dirty ones"""
        df = load_leads(spark, "data/raw/sf_leads.csv")
        assert df.count() == 8

    def test_leads_has_correct_columns(self, spark):
        """Loaded leads should have all expected columns"""
        df = load_leads(spark, "data/raw/sf_leads.csv")
        assert "lead_id" in df.columns
        assert "first_name" in df.columns
        assert "last_name" in df.columns
        assert "email" in df.columns
        assert "company" in df.columns
        assert "status" in df.columns
        assert "lead_source" in df.columns
        assert "country" in df.columns
        assert "created_date" in df.columns

    def test_leads_preserves_nulls(self, spark):
        """Bronze should NOT remove null values - that is Silver's job"""
        df = load_leads(spark, "data/raw/sf_leads.csv")
        null_emails = df.filter(df["email"].isNull()).count()
        assert null_emails > 0

    def test_leads_preserves_mixed_case(self, spark):
        """Bronze should NOT normalise casing - that is Silver's job"""
        df = load_leads(spark, "data/raw/sf_leads.csv")
        statuses = [row["status"] for row in df.collect()]
        assert "OPEN" in statuses

    def test_leads_enforces_schema(self, spark):
        """Leads should be loaded with schema from YAML not inferred"""
        df = load_leads(spark, "data/raw/sf_leads.csv")
        assert df.schema == get_leads_schema()


# ===========================
# BRONZE TASK RUN TESTS
# ===========================

class TestRunBronzeSalesforce:

    def test_run_bronze_salesforce_creates_leads_output(
        self, spark, tmp_path
    ):
        """run_bronze_salesforce should write leads parquet to output path"""
        run_bronze_salesforce(
            spark,
            leads_input="data/raw/sf_leads.csv",
            output_path=str(tmp_path)
        )
        import os
        assert os.path.exists(f"{tmp_path}/leads")

    def test_run_bronze_salesforce_output_readable(self, spark, tmp_path):
        """Written leads parquet should be readable with correct row count"""
        run_bronze_salesforce(
            spark,
            leads_input="data/raw/sf_leads.csv",
            output_path=str(tmp_path)
        )
        df = spark.read.parquet(f"{tmp_path}/leads")
        assert df.count() == 8