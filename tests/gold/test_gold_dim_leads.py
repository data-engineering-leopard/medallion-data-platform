import os
import pytest
from my_project.tasks.gold.dim_leads import DimLeadsTask, run_dim_leads


class TestBuildDimLeads:

    @pytest.fixture(scope="class")
    def dim_result(self, spark, silver_leads_schema, tmp_path_factory):
        """Build dim_leads once and reuse across tests"""
        data = [
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
        df = spark.createDataFrame(data, silver_leads_schema)
        task = DimLeadsTask(spark=spark, input_path="unused", output_path="unused")
        return task.transform(df).cache()

    def test_lead_key_is_added(self, dim_result):
        """dim_leads should have a lead_key surrogate key column"""
        assert "lead_key" in dim_result.columns

    def test_lead_key_is_unique(self, spark, silver_leads_schema):
        """Every row in dim_leads should have a unique lead_key"""
        data = [
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
        df = spark.createDataFrame(data, silver_leads_schema)
        task = DimLeadsTask(spark=spark, input_path="unused", output_path="unused")
        result = task.transform(df)
        total = result.count()
        distinct = result.select("lead_key").distinct().count()
        assert total == distinct

    def test_all_silver_columns_present(self, dim_result):
        """All silver columns should be present in dim_leads"""
        assert "lead_id" in dim_result.columns
        assert "first_name" in dim_result.columns
        assert "last_name" in dim_result.columns
        assert "email" in dim_result.columns
        assert "company" in dim_result.columns
        assert "status" in dim_result.columns
        assert "lead_source" in dim_result.columns
        assert "country" in dim_result.columns
        assert "effective_from" in dim_result.columns
        assert "effective_to" in dim_result.columns
        assert "is_current" in dim_result.columns

    def test_all_rows_preserved(self, spark, silver_leads_schema):
        """All rows including historical versions should be in dim_leads"""
        data = [
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
            ),
        ]
        df = spark.createDataFrame(data, silver_leads_schema)
        task = DimLeadsTask(spark=spark, input_path="unused", output_path="unused")
        result = task.transform(df)
        assert result.count() == 3


class TestRunDimLeads:

    @pytest.fixture(scope="class")
    def dim_leads_output(self, spark, tmp_path_factory, silver_leads_schema):
        """Run dim_leads once and reuse output across tests"""
        silver_path = str(tmp_path_factory.mktemp("silver_leads"))
        gold_path = str(tmp_path_factory.mktemp("gold_dim_leads"))

        data = [
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
        spark.createDataFrame(data, silver_leads_schema).write.mode(
            "overwrite"
        ).parquet(silver_path)

        run_dim_leads(spark, input_path=silver_path, output_path=gold_path)
        return {"spark": spark, "output_path": gold_path}

    def test_run_dim_leads_creates_output(self, dim_leads_output):
        """run_dim_leads should write parquet to output path"""
        assert os.path.exists(dim_leads_output["output_path"])

    def test_run_dim_leads_output_readable(self, dim_leads_output):
        """Output parquet should be readable with correct row count"""
        result = dim_leads_output["spark"].read.parquet(dim_leads_output["output_path"])
        assert result.count() == 1
