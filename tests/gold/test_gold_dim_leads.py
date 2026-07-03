import os
import pytest
from pyspark.sql import DataFrame
from my_project.tasks.gold.dim_leads import DimLeadsTask


# ===========================
# SHARED TEST DATA
# ===========================

SINGLE_LEAD = [
    (
        1,
        "John",
        "Smith",
        "john@acme.com",
        "Acme",
        "open",
        "web",
        "UK",
        "2024-01-10",
        None,
        None,
        None,
        True,
    )
]

TWO_VERSIONS_OF_LEAD = [
    (
        1,
        "John",
        "Smith",
        "john@old.com",
        "Acme",
        "open",
        "web",
        "UK",
        "2024-01-10",
        None,
        None,
        None,
        False,
    ),
    (
        1,
        "John",
        "Smith",
        "john@new.com",
        "Acme",
        "open",
        "web",
        "UK",
        "2024-01-10",
        None,
        None,
        None,
        True,
    ),
]

MULTIPLE_LEADS = TWO_VERSIONS_OF_LEAD + [
    (
        2,
        "Sarah",
        "Jones",
        "sarah@globex.com",
        "Globex",
        "working",
        "phone",
        "US",
        "2024-01-11",
        None,
        None,
        None,
        True,
    )
]


# ===========================
# TRANSFORM TESTS
# ===========================


class TestDimLeadsTransform:

    @pytest.fixture(scope="class")
    def task(self, spark):
        """Reusable task instance for transform tests"""
        return DimLeadsTask(spark=spark, input_path="unused", output_path="unused")

    @pytest.fixture(scope="class")
    def single_lead_result(self, spark, task, silver_leads_schema):
        """Transform a single lead — reused across multiple tests"""
        df = spark.createDataFrame(SINGLE_LEAD, silver_leads_schema)
        return task.transform(df).cache()

    def test_lead_key_is_added(self, single_lead_result):
        """dim_leads should have a lead_key surrogate key column"""
        assert "lead_key" in single_lead_result.columns

    def test_result_is_dataframe(self, single_lead_result):
        """transform() should return a DataFrame"""
        assert isinstance(single_lead_result, DataFrame)

    def test_lead_key_is_unique(self, spark, task, silver_leads_schema):
        """Every row in dim_leads should have a unique lead_key"""
        df = spark.createDataFrame(TWO_VERSIONS_OF_LEAD, silver_leads_schema)
        result = task.transform(df)
        total = result.count()
        distinct = result.select("lead_key").distinct().count()
        assert total == distinct

    def test_lead_key_is_not_null(self, single_lead_result):
        """lead_key should never be null"""
        null_count = single_lead_result.filter(
            single_lead_result["lead_key"].isNull()
        ).count()
        assert null_count == 0

    def test_all_silver_columns_present(self, single_lead_result):
        """All silver columns should be present in dim_leads"""
        expected_columns = [
            "lead_id",
            "first_name",
            "last_name",
            "email",
            "company",
            "status",
            "lead_source",
            "country",
            "effective_from",
            "effective_to",
            "is_current",
        ]
        for col in expected_columns:
            assert col in single_lead_result.columns

    def test_all_rows_preserved(self, spark, task, silver_leads_schema):
        """All rows including historical versions should be in dim_leads"""
        df = spark.createDataFrame(MULTIPLE_LEADS, silver_leads_schema)
        result = task.transform(df)
        assert result.count() == 3

    def test_lead_key_is_first_column(self, single_lead_result):
        """lead_key should be the first column"""
        assert single_lead_result.columns[0] == "lead_key"


# ===========================
# FULL TASK RUN TESTS
# ===========================


class TestDimLeadsTaskRun:

    @pytest.fixture(scope="class")
    def task_output(self, spark, tmp_path_factory, silver_leads_schema):
        """Run DimLeadsTask once and reuse output across tests"""
        silver_path = str(tmp_path_factory.mktemp("silver_leads"))
        gold_path = str(tmp_path_factory.mktemp("gold_dim_leads"))

        spark.createDataFrame(SINGLE_LEAD, silver_leads_schema).write.mode(
            "overwrite"
        ).parquet(silver_path)

        DimLeadsTask(spark=spark, input_path=silver_path, output_path=gold_path).run()

        return {"spark": spark, "output_path": gold_path}

    def test_output_path_created(self, task_output):
        """DimLeadsTask should create the output path"""
        assert os.path.exists(task_output["output_path"])

    def test_output_is_readable(self, task_output):
        """Output parquet should be readable"""
        result = task_output["spark"].read.parquet(task_output["output_path"])
        assert isinstance(result, DataFrame)

    def test_output_has_correct_row_count(self, task_output):
        """Output should contain correct number of rows"""
        result = task_output["spark"].read.parquet(task_output["output_path"])
        assert result.count() == 1

    def test_output_has_lead_key_column(self, task_output):
        """Output parquet should have lead_key column"""
        result = task_output["spark"].read.parquet(task_output["output_path"])
        assert "lead_key" in result.columns
